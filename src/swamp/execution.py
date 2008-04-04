# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
execution - contains code related to executing commands


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

from heapq import * # for minheap implementation
from itertools import imap, izip, chain

import inspect # for debugging
import math
import os
import Queue
import random
import subprocess
import time
import threading

import urllib
import urllib2

# for working around python bug http://bugs.python.org/issue1628205
import socket
from errno import EINTR

# third party imports
from SOAPpy import SOAPProxy

# swamp imports
from swamp.mapper import FileMapper # just for LocalExecutor.newInstance
from swamp import log
from swamp.partitioner import PlainPartitioner
from swamp.command import picklableList



#local module helpers:
def appendList(aDict, key, val):
    l = aDict.get(key,[])
    l.append(val)
    aDict[key] = l

def appendListMulKey(aDict, keys, val):
    for k in keys:
        l = aDict.get(k,[])
        l.append(val)
        aDict[k] = l


class NcoBinaryFinder:
    def __init__(self, config):
        self.config = config
        pass
    def __call__(self, cmd):
        # for now, always pick one nco binary,
        # regardless of netcdf4 or opendap.
        return self.config.execNcoDap + os.sep + cmd.cmd


class EarlyExecutor(threading.Thread):
    def __init__(self, executor, cmdlist, stopFunc):
        Thread.__init__(self) 
        self.executor = executor
        self.cmdList = cmdlist
        self.stopFunc = stopFunc
        pass
    def run(self):
        while not self.stopFunc():
            tok = self.executor.launch(cmd)
            retcode = self.executor.join(tok)
        # NOT FINISHED
            
class LocalExecutor:
    """
    An executor in the "new style" executes clusters rather than
    single commands.

    A fake executor version is bundled here for testing the
    clustering and the upper-level dispatcher.
    The only difference between local and fake-local execution
    is skipping the actual execution and just modeling the
    results instead."""
    
    def __init__(self, mode='fake', binaryFinder=None, filemap=None, slots=1):
        """set mode to 'fake' to provide a faking executor,
        or 'local' to use a normal local execution engine"""
        self.runningClusters = {}
        self.finishedClusters = set()
        #self.token = 0
        self.execMode = mode
        self.alive = True
        self._runningCmds = [] # (cmd, containingcluster)
        self._roots = []
        self.binaryFinder = binaryFinder
        self.filemap = filemap
        self.actual = {} # shared: not safe across tasks
        self.slots = slots
        if mode == 'fake':
            self._initFakeMode()
        elif mode == 'local':
            self._initLocalMode()
        else:
            log.error("Serious Error: constructed mode-less executor")
        self.thread = threading.Thread(target=self._threadRun, args=())
        self.thread.start()
        pass

    def _initFakeMode(self):
        self._launch = self._launchFake
        self._threadRun = self._threadRunFake
        self.avgCmdTime = 0.5# avg exec time per command
        self.tickSize = 1.0/(self.avgCmdTime*10) # ten ticks per mean time
        self.pCmdExec = 1 - math.exp(-self.tickSize/self.avgCmdTime)
        self._availFiles = set()
        self.idleTicks = 0
        self._enqueue = lambda cmd,clus: self._roots.append((cmd,clus))
        
    def _initLocalMode(self):
        self._launch = self._launchLocal
        self._threadRun = self._threadRunLocal
        self.finished = set() ## DEBUG: REMOVE later
        self.cmdQueue = Queue.Queue()
        self.cmdsEnqueued = set()
        self.cmdsEnqueuedLock = threading.Lock()
        self.stateLock = threading.Lock()
        self._fetchLock = threading.Lock()
        self._startPool(self.slots)
        self._enqueue = lambda cmd,clus: self.cmdQueue.put((cmd,clus))
        pass
    
    def _threadRun(self):
        "Placeholder: runtime alias of _threadRunFake or _threadRunLocal"
        raise "---ERROR--- unmapped call to threadRun"

    def _threadRunLocal(self):
        # the non-fake version doesn't really have to do anything but
        # wait on the pids of its spawned processes.  And when it's
        # not waiting, maybe it can wait on a condition or something.

        # The concept of being 'alive' should exist in both real and
        # fake modes.

        # Execution uses a thread pool, where threads exist solely to
        # wakeup when children processes terminate. A previous
        # architecture used async spawning, but had to resort to
        # periodic waitpid calls. 

        while self.alive:
            # dispatch as we are able.
            # shove everything we can on the queue.
            self._queueAllReady()
            # Is there a real reason for doing this more
            # than once? What if the original queuing action puts
            # things on the queue directly?
            time.sleep(2)
            

        pass

    def _startPool(self, num):
         self._pool = map(lambda n:
                          threading.Thread(target=self._process),
                          range(num))
         map(lambda t: t.start(), self._pool)

    def _process(self):
        """Main loop for the pool worker"""
        while self.alive:
            ctuple = self.cmdQueue.get()

            if not self.alive: # still alive?
                break
            (cmd,clus) = ctuple
            code = self._launch(cmd, locations=clus.exec_inputLocs)
            if code != 0:
                self._failCmd(ctuple, code)
                break # Do not continue
                
            #FIXME: check for error.
            self._graduateCmd(ctuple)

        pass
    
    def _killPool(self):
        # turn off alive, and poison the queue
        self.alive = False
        map(lambda t: self.cmdQueue.put(None), self._pool)
   
        
    def _threadRunFake(self):
        open("/dev/stderr","w").write( "start fake")
        while self.alive:
            # while alive, we pretend to finish commands.
            # "finish"
            # given cmdrate represents probabalistic rate, so for each "running" cmd, decide whether or not it has finished, and then process its finish.
            self._fakeFinishExecution()
            # "dispatch":
            # if we have empty slots, then run a ready cmd
            # pull a cmd from a cluster and put it in the running list.
            # sleep until next cycle.
            emptyslots = self.slots - len(self._runningCmds)
            if emptyslots > 0:
                self._dispatchSlots(emptyslots)
            
            # sleep 
            time.sleep(self.tickSize)
            print "tick[", self.idleTicks, "]",
            self.idleTicks += self.tickSize
            if self.idleTicks >= self.avgCmdTime*5:
                print "death by boredom"
                self.alive = False 

    def _dispatchSlots(self, slots):
        dispatched = 0
        for x in range(slots):
            if self._roots:
                cmdTuple = self._roots.pop()
                (cmd, clus) = cmdTuple
                self._launch(cmd)
                self._runningCmds.append(cmdTuple)
                dispatched += 1
            else:
                break
        return dispatched

    def _queueAllReady(self):
        while self._roots:
            self._enqueue(self._roots.pop())

            
    def _fakeFinishExecution(self):
        finishing = filter(lambda x: random.random() < self.pCmdExec, self._runningCmds)
        map(self._fakeGraduate, finishing)


    def _fakeGraduate(self, cmdTuple):
        # mark 'finished' to free up slots
        self._runningCmds.remove(cmdTuple)
        (cmd, cluster) = cmdTuple
        # Fabricate actual outputs (phys filename, size)
        cmd.actualOutputs = map(lambda x: (x, "bogus_"+x, 1000),
                                cmd.outputs)
        self._graduateCmd(cmdTuple)
        self.idleTicks = 0
        pass

    @staticmethod
    def newInstance(config):
        return LocalExecutor('local', NcoBinaryFinder(config),
                             FileMapper("swamp%d"%os.getpid(),
                                        config.execSourcePath,
                                        config.execScratchPath,
                                        config.execBulkPath),
                             config.execLocalSlots)

    def _failCmd(self, cmdTuple, code):
        self._touchUrl(cmdTuple[0].callbackUrl[1])
        
        
    def _graduateCmd(self, cmdTuple):

        self.stateLock.acquire()
        # fix internal structures to be consistent:
        cmd = cmdTuple[0]
        cluster = cmdTuple[1]
        # Update cluster status
        cluster.exec_finishedCmds.add(cmd)
        self.finished.add(cmd) ## DEBUG. REMOVE later.

        for x in chain(cmd.actualOutputs, cmd.rFetchedFiles):
            self.actual[x[0]] = x[1]
        if cluster.exec_outputPatch:
            cmd.actualOutputs = map(lambda t:
                                    (t[0], cluster.exec_outputPatch(t[1]),
                                            os.stat(t[1]).st_size,t[1]),
                                    chain(cmd.actualOutputs, cmd.rFetchedFiles))
        log.debug("Local graduation with outputs: %s" %(str(cmd.actualOutputs)))
        # put children on root queue if ready
        newready = set()
        for c in cmd.children:
            if c not in cluster: # don't dispatch outside my cluster
                continue
            ready = reduce(lambda x,y: x and y,
                           map(lambda f: f in self.actual, c.inputs),
                           True)
            #print "inputs",c.inputs, "availfiles",self.actual.keys(),ready
            #print "ready?", ready
            if ready:
                newready.add(c)
                
                if not reduce(lambda a,b: a and b, map(lambda p: p in self.finished,  c.parents), True):
                    partialprod =  map(lambda f: f in self.actual, c.inputs)
                    print "Are ", c.inputs, "in", self.actual.keys(), partialprod, id(c)
                    print id(c), "----CONFLICT--- files 'ready' but parents not", id(cmd)

        # Protect enqueuing since threads can race here
        # (2 parents-> 1 child)
        self.cmdsEnqueuedLock.acquire()
        enq = newready.difference(self.cmdsEnqueued)
        self.cmdsEnqueued.update(enq)
        self.cmdsEnqueuedLock.release()
        map(lambda c: self._enqueue(c,cluster), enq)
        
        # report results
        self._touchUrl(cmd.callbackUrl[0], cmd.actualOutputs)
        if len(cluster.exec_finishedCmds) == len(cluster.cmds):
            # call cluster graduation.
            func = self.runningClusters.pop(cluster)
            func()
            self.finishedClusters.add(cluster)
        self.stateLock.release()
        
    def _touchUrl(self, url, actualOutputs):
        if isinstance(url, type(lambda : True)):
            return url(None)
        try:
            pkg = dict([(x[0],(x[1],x[2])) for x in actualOutputs])
            data = urllib.urlencode(pkg)
            f = urllib2.urlopen(url + "?"+data)
            f.read() # read result, discard for now
        except KeyError:
            return False
        return True
    
    def needsWork(self):
        # cache this.
        return len(self.runningClusters) < self.slots

    def dispatch(self, cluster, registerFunc, finishFunc, outputPatch=None,
                 locations=[]):
        # registerFunc is f(command, hook, isLocal)
        # registerFunc returns a tuple of success/fail
        # registerFunc should return funcs if local
        # and urls if remote.
        # for local executor, can use null hook.
        # finishFunc is a function to call when the cluster is finished.
        # outputPatch= f:pathname -> patchedNameForCommand
        # dispatch(...) needs to be 'nonblocking'
        if registerFunc:
            for cmd in cluster:
                urls = registerFunc(cmd, lambda c,f,cu: None, self, True)
                cmd.callbackUrl = urls

        self.runningClusters[cluster] = finishFunc
        cluster.exec_finishedCmds = set()
        cluster.exec_outputPatch = outputPatch
        if locations:
            cluster.exec_inputLocs = locations
        elif not hasattr(cluster, 'exec_inputLocs'):
            cluster.exec_inputLocs = []
        map(lambda c: self._enqueue(c,cluster), cluster.roots)

    def _launch(self, cmd, locations=[]):
        "Placeholder: runtime alias of launchFake or launchLocal"
        raise "---ERROR--- unmapped call to launch"

        
    def _launchFake(self, cmd, locations = []):
        print "fakelaunch tuple",cmd
        cmdLine = cmd.makeCommandLine(lambda x: x, lambda y:y)
        print "fakeran",cmdLine
        #self.token += 1
        #self.running.append(self.fakeToken)
        #return self.fakeToken
        pass

    def _launchLocal(self, cmd, locations=[]):
        if not reduce(lambda a,b: a and b, map(lambda c: c in self.finished,  cmd.parents), True):
            print id(cmd), "was queued, but isn't ready!"
        # make sure our inputs are ready
        missing = filter(lambda f: not self.filemap.existsForRead(f),
                         cmd.inputs)
        cmd.rFetchedFiles = []
        if locations:
            cmd.inputSrcs = locations
        if missing:
            fetched = self._fetchLogicals(missing, cmd.inputSrcs)
            cmd.rFetchedFiles = fetched
            fetched = self._verifyLogicals(set(cmd.inputs).difference(missing))
        cmdLine = cmd.makeCommandLine(self.filemap.mapReadFile,
                                      self.filemap.mapWriteFile)
        #Make room for outputs (shouldn't be needed)
        self._clearFiles(map(lambda t: t[1], cmd.actualOutputs))

        proc = subprocess.Popen(executable=self.binaryFinder(cmd),
                         args=cmdLine,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
        cmd.exec_output = proc.communicate()[0] #Save output (future use)
        code = proc.returncode
        
        # consider:
        # exitcode=subprocess.call(executable=binPath,
        # args=arglist, stdout=some filehandle with output,
        # stderrr= samefilehandle)
        return code

    def _verifyLogicals(self, logicals):
        if len(logicals) == 0:
            return []
        for f in logicals:
            while f == self.fetchFile:
                time.sleep(0.1)
        return []
      
    def _fetchLogicals(self, logicals, srcs):
        fetched = []
        if len(logicals) == 0:
            return []
        log.info("need fetch for %s from %s" %(str(logicals),str(srcs)))
        
        d = dict(srcs)
        for lf in logicals:
            self._fetchLock.acquire()
            if self.filemap.existsForRead(lf):
                self._fetchLock.release()
                log.debug("satisfied by other thread")
                continue
            self.fetchFile = lf
            #phy = self.filemap.mapBulkFile(lf) # 
            phy = self.filemap.mapWriteFile(lf)
            # FIXME NOW: d[lf] not always valid!!!
            log.debug("fetching %s from %s" % (lf, d[lf]))
            self._fetchPhysical(phy, d[lf])
            fetched.append((lf, phy))
            self.fetchFile = None
            self._fetchLock.release()
        return fetched

    def _fetchPhysical(self, physical, url):
        #urllib.urlretrieve(d[lf], phy)
        # urlretrieve dies on interrupt signals
        # Use curl: fail silently, silence output, write to file
        tries = 1
        maxTries = 3
        pid = None
        while pid is None:
            try:
                pid = os.spawnv(os.P_NOWAIT, '/usr/bin/curl',
                                ['curl', "-f", "-s", "-o", physical, url])
            except OSError, e:
                if not (e.errno == 513):
                    raise
                pass #retry on ERESTARTNOINTR
        rc = None
        while rc is None:
            try:
                (p,rc) = os.waitpid(pid,0)
                rc = os.WEXITSTATUS(rc)
            except OSError, e:
                if not (e.errno == 4):
                    raise
                log.info("Retry, got errno 4 (interrupted syscall)")
                continue
            if rc != 0:
                raise StandardError("error fetching %s (curl code=%d)" %
                                    (url, rc))

    def discardFilesIfHosted(self, files):
        # We tolerate being called with files we don't host.
        hosted = filter(lambda f: f in self.actual, files)
        self._discardHosted(hosted)

        return 

    def discardAllHosted(self):
        return self._discardHosted(self.actual.keys())

    def _discardHosted(self, files):
        # need to map to actual locations first.
        mappedfiles = map(self.filemap.mapReadFile, files)
        map(self.filemap.discardLogical, files)
        map(self.actual.pop, files)

        
        
    def _clearFiles(self, filelist):
        for f in filelist:
            if os.access(f, os.F_OK):
                if os.access(f, os.W_OK):
                    os.remove(f)
                else:
                    raise StandardError("Tried to unlink read-only %s"
                                        % (fname))
            pass
        pass

    def forceJoin(self):
        self.alive = False
        if self.execMode != 'fake':
            self._killPool()
        if self._runningCmds:
            # If thread is running, let it finish.
            # Perhaps reduce its timers to make it finish faster
            # otherwise, just terminate.
            self.thread.join()
            
    pass

class RemoteExecutor:
    def __init__(self, url, slots):
        """ url: SOAP url for SWAMP slave
            slots: max number of running slots

            RemoteExecutor adapts a remote worker's execution resources
            so that they may be used by a parallel dispatcher.
            """
        self.url = url
        self.slots = slots
        self.rpc = SOAPProxy(url)
        log.debug("reset slave at %s with %d slots" %(url,slots))
        try:
            self.rpc.reset()
        except Exception, e:
            import traceback, sys
            tb_list = traceback.format_exception(*sys.exc_info())
            msg =  "".join(tb_list)
            raise StandardError("can't connect to "+url+str(msg))
        self.runningClusters = set()
        self.finishedClusters = set()
        self.actual = {}
        self.cmds = {}
        pass
    
    def dispatch(self, cluster, registerFunc, finishFunc, outputPatch=None,
                 locations=[]):
        
        funcs = map(lambda c:
                    setattr(c,
                            'callbackUrl',
                            registerFunc(c, lambda cm,f,cu: self._graduateCmd(c, cluster, f,cu),
                                         self, False)), cluster.cmds)

        
        # Take the cluster, dispatch it
        # Pass the cluster, including its commands
        # Cluster should include the URLs.
        # This bothers me that we can't share the mgmt code with
        # the top-level execution.
        cluster.exec_inputLocs = locations
        # Do whatever pickling we need:
        # For each command, remove external parents/children, because our
        # helper shouldn't worry about them.
        pc = cluster.pickleSelf(picklableList)

        # Fill-in management fields afterwards so they don't get pickled.
        # Register callback URLs for each command
        cluster.exec_finishCount = 0
        # Set finishing function for the cluster
        cluster.exec_finishFunc = finishFunc        
        self.rpc.processCluster(pc)
        
        pass
    
    def forceJoin(self):
        pass

    def needsWork(self):
        return len(self.runningClusters) < self.slots

    def discardFilesIfHosted(self, fileList):
        """fileList: iterable of logical filenames to discard"""
        hosted = filter(lambda f: f in self.actual, fileList)
        if hosted:
            self._discardHosted(hosted)

    def discardAllHosted(self):
        return self._discardHosted(self.actual.keys())

    def _discardHosted(self, files):
        log.debug("req discard of %s on %s" %(str(files), self.url))
        map(self.actual.pop, files)
        self.rpc.discardFiles(files)

        




    def _graduateCmd(self, cmd, cluster, fail, custom):
        # Cluster callback needs to passthrough this object,
        # so that we know when a cluster is finished, otherwise
        # we can't mark ourselves as needing work.
        # Alternatively, "need for work" can be defined by polling
        # and checking some mix of processing load and queue length.

        # custom is of the form: 
        #{'filename.nc': ["('http://host:8082/pathname/munged.nc', 1234)"]}

        
        #if fail:
            # FIXME: Do the right thing when things fail
            #pass
        # handle actualOutputs
        print "graduating with custom=",custom
        if custom:
            def unbundle(x):
                props = x[1][0] # Want to do eval(x[1][0]), but it's unsafe.
                props = props[1:-1].split(', ') # Drop the parens and split.
                
                return (x[0], props[0][1:-1], int(props[1]))
            cmd.actualOutputs = [unbundle(x) for x in custom.items()]
            log.debug("Remote cmd produced %s" %(str(cmd.actualOutputs)))
            self.actual.update([(x[0],x[1]) for x in cmd.actualOutputs])
        else:
            cmd.actualOutputs = []
            log.warning("Remote cmd produced no outputs")
        
        # Do cluster bookkeeping    
        cluster.exec_finishCount += 1
        if cluster.exec_finishCount == len(cluster.cmds):
            cluster.exec_finishFunc()
            self.runningClusters.discard(cluster) #discard supresses errors.
            self.finishedClusters.add(cluster)
        pass
        
        



######################################################################
# Things to help debugging
######################################################################
def makeTestConfig():
    class TestConfig:
        def __init__(self):
            self.execNcoDap = "/usr/bin"
    return TestConfig()

def makeFakeExecutor():
    return NewLocalExecutor(mode='fake')

class PseudoFactory:
    """a stand-in for a command factory based on the provided
    list of commands"""
    def __init__(self, cmdList):
        def tuples():
            for c in cmdList:
                for i in c.inputs:
                    yield (i,c)
        d = {}
        map(lambda t: d.setdefault(t[0],[]).append(t[1]), tuples())
        self.commandByLogicalIn = d
        pass
        
                
            
def loadCmds(filename):
    import cPickle as pickle
    return pickle.load(open(filename))

def makeLocalExec(config):
    return NewLocalExecutor(mode='local', binaryFinder=NcoBinaryFinder(config),
                            filemap=FileMapper("swamp%d"%os.getpid(),
                                               "./s",
                                               "./p",
                                               "./b" ),
                            slots=2)
    

def testDispatcher():
    config = makeTestConfig()
    return testRun(config, [makeFakeExecutor()])


def testRun(config, execu):
    e = execu
    import swamp.scheduler as scheduler
    pd = scheduler.NewParallelDispatcher(config, e)
    cmds = loadCmds("exectestCmds.pypickle")
    pf = PseudoFactory(cmds)
    map(lambda c: setattr(c,"factory",pf), cmds)
    pd.dispatchAll(cmds)
    print "Running, no stops forced now."
    try:
        time.sleep(3)
    except:
        pass
    #e[0].forceJoin()

    while not pd.idle():
        time.sleep(1)
    e[0].forceJoin()

    print "cmds exec'd", pd.count

def testLocalDispatch():
    config = makeTestConfig()
    return testRun(config, [makeLocalExec(config)])
    pass
