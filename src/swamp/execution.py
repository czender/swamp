# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
execution - contains code related to executing commands


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

from heapq import * # for minheap implementation
import itertools
import inspect # for debugging
import math
import os
import Queue
import random
import time
import threading

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
            

class ParallelDispatcher:
    def __init__(self, config, executorList):
        self.config = config
        self.executors = executorList
        self.finished = {}
        self.okayToReap = True
        # not okay to declare file death until everything is parsed.
        self.execLocation = {} # logicalout -> executor
        self.sleepTime = 0.100 # originally set at 0.200
        self.running = {} # (e,etoken) -> cmd
        self.execPollRR = None
        self.execDispatchRR = None
        self.result = None
        pass

    def fileLoc(self, file):
        """ return url of logical output file"""
        execs = self.execLocation[file]
        if len(execs) > 0:
            try:
                return execs[0].actual[file] # return the first one
            except KeyError:
                i = 0
                emap = {}
                msg = ["#Key error for %s" % (file),
                       "badkey = '%s'" %(file)]
                for e in self.executors:
                    nicename = "e%dactual" % i
                    emap[e] = nicename
                    msg.append("%s = %s" %(nicename, str(e.actual)))
                    i += 1
                msg.append("#Key %s maps to %s" %(file,
                                                 map(lambda e: emap[e],
                                                     self.execLocation[file])))
                locstr = str(self.execLocation)
                for (k,v) in emap.items():
                    locstr = locstr.replace(str(k),v)
                msg.append("eLoc = " + locstr) 
                open("mykeyerror.py","w").write("\n".join(msg))
                log.error("writing debug file to mykeyerror.py")
                execs[0].actual[file] # re-raise error.
        else:
            return None
        
    def isDead(self, file, consumingCmd):
        # Find the producing cmd, see if all its children are finished.
        return 0 == len(filter(lambda c: c not in self.finished,
                               consumingCmd.factory.commandByLogicalIn[file]))

    def isReady(self, cmd):
        # if there are no unfinished parents...
        return 0 == len(filter(lambda c: c not in self.finished, cmd.parents))

    def _allBusy(self):
        for e in self.executors:
            if not e.busy():
                return False
        return True
        

    def findReady(self, queued):
        return filter(self.isReady, queued)

    def extractReady(self, queued):
        """returns a tuple (ready,queued)
        filter is elegant to generate r, but updating q is O(n_r*n_q),
        whereas this generates r and q in O(n_q)
        """
        r = []
        q = []
        for c in queued:
            if self.isReady(c):
                r.append(c)
            else:
                q.append(c)
        return (r,q)
        
        
    def findMadeReady(self, justFinished):
        """Generally, len(justFinished.children) << len(queued).
        n_j * n_q'  << n_q, since n_j << n_q
        and n_q'(position number of justfinishedchild << n_q, so
        it's generally cheaper just to find+remove the ready
        elements instead of iterating over the entire list."""
        assert justFinished in self.finished
        return filter(self.isReady, justFinished.children)
    
    def dispatch(self, cmd):
        log.info("dispatching %s %d" %(cmd.cmd, cmd.referenceLineNum))
        inputLocs = map(lambda f:(f, self.fileLoc(f)),
                        cmd.inputsWithParents)
        # look for a more appropriate executor:
        # pick host of first file if not busy.
        execs = map(lambda f:self.execLocation[f], cmd.inputsWithParents)
        executor = None
        for e in execs:
            if not e[0].busy():
                executor = e[0]
                break
        if not executor:
            executor = self._nextFreeExecutor()
        etoken = executor.launch(cmd, inputLocs)
        self.running[(executor, etoken)] = cmd

    def releaseFiles(self, files):
        if len(files) == 0:
            return
        log.debug("ready to delete " + str(files))
        # collect by executors

        if False: # old way deletes files by where they were produced
            eList = {}
            map(lambda f: appendListMulKey(eList, self.execLocation[f],f),
                files)
            for (k,v) in eList.items():
                log.debug("Discard: "+str(k)+"  :  "+str(v))
                k.discardFiles(v)
        else: # new way looks at where files reside (may have been downloaded)
            map(lambda e: e.discardFilesIfHosted(files), self.executors)
            
            
        # cleanup execLocation
        map(self.execLocation.pop, files)
#        map(lambda f: map(lambda e: e.discardFile(f),
#                          self.execLocation[f]),
#            files)
        pass


    def _graduate(self, token, code, hook):
        cmd = self.running.pop(token)
        if code != 0:
            origline = ' '.join([cmd.cmd] + map(lambda t: ' '.join(t), cmd.argList) + cmd.leftover)
            s = "Bad return code %s from cmdline %s %d outs=%s" % (
                code, origline, cmd.referenceLineNum, str(cmd.outputs))
            log.error(s)
            # For nicer handling, we should find the original command line
            # and pass it back as the failing line (+ line number)
            # It would be nice to trap the stderr for that command, but that
            # can be done later, since it's a different set of pipes
            # to connect.

            self.result = "Error at line %d : %s" %(cmd.referenceLineNum,
                                                    origline)
            return
            #raise StandardError(s)
        else:
            # figure out which one finished, and graduate it.
            self.finished[cmd] = code
            log.debug("graduating %s %d" %(cmd.cmd,
                                           cmd.referenceLineNum))
            # update the readylist
            newready = self.findMadeReady(cmd)
            map(lambda x: heappush(self.ready,x), newready)
            # delete consumed files.
            if self.okayToReap:
                self.releaseFiles(filter(lambda f: self.isDead(f, cmd),
                                         cmd.inputsWithParents))
            e = token[0] # token is (executor, etoken)
            map(lambda o: appendList(self.execLocation, o, e), cmd.outputs)
            execs = self.execLocation[cmd.outputs[0]]

            #self._publishFiles(filter(lambda o: self.isOutput(o),
            #                         cmd.outputs))
            #execs[0].actual[cmd.outputs[0]] # FIXME: can I delete this?
            # hardcoded for now.

            # call this to migrate files to final resting places.
            # Use a generic hook passed from above.
            hook(cmd)
            pass

    def _pollAny(self, hook):
        for i in range(len(self.executors)):
            e = self.execPollRR.next()
            # should be faster to iterate over self.executors
            # than self.running
            r = e.pollAny()
            if r is not None:
                #log.debug("dispatcher pollany ok: "+str(r))
                self._graduate((e, r[0]), r[1], hook)
                return ((e, r[0]), r[1])
        return None

    def _waitAnyExecutor(self, hook):
        while True:
            r = self._pollAny(hook)
            if r is not None:
                return r
            time.sleep(self.sleepTime)

        
    def _nextFreeExecutor(self):
        """Return next free executor. Try to pursue a policy of
        packing nodes tight so as to maximize locality.
        We'll go fancier some other time."""
        for ei in self.executors:
            e = self.execDispatchRR.next()
            if not e.busy():
                return e
        return None

    def dispatchAll(self, cmdList, hook=lambda f:None):
        #log.debug("dispatching cmdlist="+str(cmdList))
        self.running = {} # token -> cmd
        self.execPollRR = itertools.cycle(self.executors)
        self.execDispatchRR = itertools.cycle(self.executors)
        (self.ready, self.queued) = self.extractReady(cmdList)

        # Consider 'processor affinity' to reduce data migration.
        while True:
            # if there are free slots in an executor, run what's ready
            if self._allBusy() or not self.ready:
                if not self.running: # done!
                    break
                log.debug("waiting")
                r = self._waitAnyExecutor(hook)
                log.debug("wakeup! %s" %(str(r)))
                if r[1] != 0:
                    # let self.result bubble up.
                    return
                #self._graduate(token, code)
            else:
                # not busy + jobs to run, so 'make it so'
                cmd = heappop(self.ready)
                self.dispatch(cmd)
            continue # redundant, but safe
        # If we got here, we're finished.
        self.result = True
        #log.debug("parallel dispatcher finish, with fileloc: " + self.fileLoc)
        pass # end def dispatchAll

    def earlyStart(self, cmdlist):
        # NOT FINISHED
        pass
    pass # end class ParallelDispatcher




class FakeExecutor:
    def __init__(self):
        self.running = []
        self.fakeToken = 0
        pass
    def launch(self, cmd, locations = []):
        cmdLine = cmd.makeCommandLine(lambda x: x, lambda y:y)
        print "fakeran",cmdLine
        self.fakeToken += 1
        self.running.append(self.fakeToken)
        return self.fakeToken
    def join(self, token):
        if token in self.running:
            self.running.remove(token)
            return True  # return False upon try-but-fail
        else:
            raise StandardError("Tried to join non-running job")
    pass

# ============================================================


class NewLocalExecutor:
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
        self.actual = {}

        if mode == 'fake':
            self._initFakeMode()
        else:
            self._initLocalMode()
        self.thread = threading.Thread(target=self._threadRun, args=())
        self.thread.start()
        pass

    def _initFakeMode(self):
        self._launch = self._launchFake
        self._threadRun = self._threadRunFake
        self.slots = 2
        self.avgCmdTime = 0.5# avg exec time per command
        self.tickSize = 1.0/(self.avgCmdTime*10) # ten ticks per mean time
        self.pCmdExec = 1 - math.exp(-self.tickSize/self.avgCmdTime)
        self._availFiles = set()
        self.idleTicks = 0
        self._enqueue = lambda cmd,clus: self._roots.append((cmd,clus))
        
    def _initLocalMode(self):
        self._launch = self._launchLocal
        self._threadRun = self._threadRunLocal
        self.cmdQueue = Queue.Queue()
        self.slots = 2 # FIXME: pull from config
        self._startPool(self.slots)
        self._enqueue = lambda cmd,clus: self.cmdQueue.put((cmd,clus))
        def eee(c,cc):
            print "enqueue", id(c)
            self.cmdQueue.put((c,cc))
        self._enqueue = eee
        pass
    
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
            print "mgmt queuing ready", len(self._roots)
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
        print "thread birth"
        while self.alive:
            ctuple = self.cmdQueue.get()
            if ctuple: print "thread pulled",id(ctuple[0])
            if not self.alive: # still alive?
                break
            print "dispatching cmdtuple",ctuple
            (cmd,clus) = ctuple
            code = self._launch(cmd)
            #FIXME: check for error.
            self._graduateCmd(ctuple)

        pass
    
    def _killPool(self):
        # turn off alive, and poison the queue
        self.alive = False
        map(lambda t: self.cmdQueue.put(None), self._pool)
   
    def _runCommand(self, binPath, arglist):
        print "asked to dispatch ", cmd
        # use errno-513-resistant method of execution.
        exitcode = None
        while exitcode is None:
            try:
                exitcode = os.spawnv(os.P_WAIT, binPath, arglist)
            except OSError, e:
                if not (e.errno == 513):
                    raise
                pass #retry on ERESTARTNOINTR
        
        # consider:
        # exitcode=subprocess.call(executable=binPath,
        # args=arglist, stdout=some filehandle with output,
        # stderrr= samefilehandle)
        return exitcode

    def _threadRun(self):
        "Placeholder: runtime alias of _threadRunFake or _threadRunLocal"
        raise "---ERROR--- unmapped call to threadRun"


        
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
            print "kickstart"
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
    def _newGraduate(self, cmdTuple):
        
        pass
        
    def _graduateCmd(self, cmdTuple):
        # fix internal structures to be consistent:
        cmd = cmdTuple[0]
        cluster = cmdTuple[1]
        # Update cluster status
        cluster.exec_finishedCmds.add(cmd)

        for x in cmd.actualOutputs:
            self.actual[x[0]] = x[1]

        # put children on root queue if ready
        for c in cmd.children:
            if c not in cluster: # don't dispatch outside my cluster
                continue
            #print "inputs",c.inputs, "availfiles",self._availFiles
            ready = reduce(lambda x,y: x and y,
                           map(lambda f: f in self.actual, c.inputs),
                           True)
            #print "ready?", ready
            if ready:
                print "parent",id(cmd),"enqueuing",id(c)
                self._enqueue(c,cluster)
        
        # report results
        self._touchUrl(cmd._callbackUrl[0])
        if len(cluster.exec_finishedCmds) == len(cluster.cmds):
            # call cluster graduation.
            print "popping",cluster
            func = self.runningClusters.pop(cluster)
            func()
            self.finishedClusters.add(cluster)
            
        
    def _touchUrl(self, url):
        if isinstance(url, type(lambda : True)):
            print "calling!"
            return url()
        try:
            f = urllib2.urlopen(url)
            f.read() # read result, discard for now
        except:
            return False
        return True
    
    def needsWork(self):
        # cache this.
        return len(self.runningClusters) < self.slots

    def dispatch(self, cluster, registerFunc, finishFunc):
        # registerFunc is f(command, hook, isLocal)
        # registerFunc returns a tuple of success/fail
        # registerFunc should return funcs if local
        # and urls if remote.
        # for local executor, can use null hook.
        # finishFunc is a function to call when the cluster is finished.
        print "dispatching", cluster
        for cmd in cluster:
            urls = registerFunc(cmd, lambda c,f: None, self, True)
            cmd._callbackUrl = urls
        print "adding cluster",cluster
        self.runningClusters[cluster] = finishFunc
        cluster.exec_finishedCmds = set()
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
        # make sure our inputs are ready
        missing = filter(lambda f: not self.filemap.existsForRead(f),
                         cmd.inputs)
        if locations:
            cmd.inputSrcs = locations
        if False and missing:
            fetched = self._fetchLogicals(missing, cmd.inputSrcs)
            cmd.rFetchedFiles = fetched
            fetched = self._verifyLogicals(set(cmd.inputs).difference(missing))
        cmdLine = cmd.makeCommandLine(self.filemap.mapReadFile,
                                      self.filemap.mapWriteFile)
        #Make room for outputs (shouldn't be needed)
        self._clearFiles(map(lambda t: t[1], cmd.actualOutputs))
        
    def _fetchLogicals(self, logicals, srcs):
        fetched = []
        if len(logicals) == 0:
            return []
        log.info("need fetch for %s from %s" %(str(logicals),str(srcs)))
        
        d = dict(srcs)
        for lf in logicals:
            self.fetchLock.acquire()
            if self.filemap.existsForRead(lf):
                self.fetchLock.release()
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
            self.fetchLock.release()
        return fetched

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

    

class LocalExecutor:
    def __init__(self, binaryFinder, filemap, slots=1):
        self.binaryFinder = binaryFinder
        self.filemap = filemap
        self.slots = slots
        self.running = {}
        self.finished = {}
        self.cmds = {}
        self.token = 10
        self.tokenLock = threading.Lock()
        self.fetchLock = threading.Lock()
        self.fetchFile = None
        self.rFetchedFiles = {}
        self.pollCache = []
        self.actual = {}
        pass

    @staticmethod
    def newInstance(config):
        return LocalExecutor(NcoBinaryFinder(config),
                             FileMapper("swamp%d"%os.getpid(),
                                        config.execSourcePath,
                                        config.execScratchPath,
                                        config.execBulkPath),
                             config.execLocalSlots)
    def busy(self):
        # soon, we should put code here to check for process finishes and
        # cache their results.
        log.debug("Exec(%d) busy %d/%d" %(os.getpid(),
                                          len(self.running), self.slots))
        return len(self.running) >= self.slots

    def launch(self, cmd, locations=[]):
        self.tokenLock.acquire()
        self.token += 1
        token = self.token
        self.tokenLock.release()
        # make sure our inputs are ready
        missing = filter(lambda f: not self.filemap.existsForRead(f),
                         cmd.inputs)
        fetched = self._fetchLogicals(missing, cmd.inputSrcs)
        self.rFetchedFiles[token] = fetched
        # doublecheck that remaining logicals are available.
        fetched = self._verifyLogicals(set(cmd.inputs).difference(missing))
        self.rFetchedFiles[token] += fetched
        
        cmdLine = cmd.makeCommandLine(self.filemap.mapReadFile,
                                      self.filemap.mapWriteFile)
        log.debug("%d-exec-> %s" % (token," ".join(cmdLine)))
        # make sure there's room to write the output
        #log.debug("clearing to make room for writing (should be using concretefiles")
        self.clearFiles(map(lambda t: t[1], cmd.actualOutputs))
        pid = self.resistErrno513(os.P_NOWAIT,
                                  self.binaryFinder(cmd), cmdLine)
        log.debug("child pid: "+str(pid))

        self.running[token] = pid
        log.debug("LocalExec added token %d" %token)
        self.cmds[token] = cmd
        return token

    def poll(self, token):
        if self.pollCache:
            #for now, be a little wasteful
            for i in range(len(self.pollCache)):
                if token == t[i][0]:
                    return self.pollCache.pop(t[i][1])
        return self._poll(token)
                    

    def _poll(self, token):
        if token in self.finished:
            return self.finished[token]
        if token in self.running:
            pid = self.running[token]
            try:
                (pid2, status) = os.waitpid(pid,os.WNOHANG)
                
                if (pid2 != 0):
                    code = os.WEXITSTATUS(status)
                    self._graduate(token, code)
                    #log.debug("LocalExec returning %d -> %d"%(pid,code))
                    return code
                else:
                    return None
            except OSError:
                self._graduate(token, -1) # Wait fault: report error
                return -1
                
        else:
            raise StandardError("Tried to poll non-running job")

    def pollAny(self):
        if self.pollCache:
            return self.pollCache.pop()
        f = self.pollAll()
        if f:
            #log.debug("pollall ok: "+str(f))
            top = f.pop()
            self.pollCache += f
            return top
        return None

    def pollAll(self):
        fins = []
        for (token, pid) in self.running.items():
            code = self._poll(token)
            if code == 0:
                fins.append((token,code))
            elif isinstance(code, int):
                log.debug("token %s gave: %s, what should I do?" %
                          (str(token),str(code)))
                fins.append((token,code)) # special handling for fail codes?
        if fins:
            return fins
        return None

    def join(self, token):
        if token in self.running:
            pid = self.running[token]
            (pid, status) = os.waitpid(pid,0)
            
            log.debug("got "+str(pid)+" " +str(status)+"after spawning")
            if os.WIFEXITED(status):
                status = os.WEXITSTATUS(status)
            else:
                status = -1
            self._graduate(token, status)
            #self.finished[token] = status
            return status
        else:
            raise StandardError("Tried to join non-running job")
    def actualOuts(self, token):
        return self.cmds[token].actualOutputs

    def fetchedSrcs(self,token):
        return self.rFetchedFiles[token]

    def discardFile(self, f):
        self.filemap.discardLogical(f)

    def discardFilesIfHosted(self, files):
        # need to map to actual locations first.
        mappedfiles = map(self.filemap.mapReadFile, files)
        return self.clearFiles(mappedfiles)

    def clearFiles(self, filelist):
        for f in filelist:
            if os.access(f, os.F_OK):
                if os.access(f, os.W_OK):
                    os.remove(f)
                else:
                    raise StandardError("Tried to unlink read-only %s"
                                        % (fname))
            pass
        pass

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
            self.fetchLock.acquire()
            if self.filemap.existsForRead(lf):
                self.fetchLock.release()
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
            self.fetchLock.release()
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

    def _graduate(self, token, retcode):
        ## fix this! pulled from remoteexecutor (1/2 finish)
        #print inspect.stack()

        self.finished[token] = retcode
        self.running.pop(token)

        cmd = self.cmds[token]
        if retcode == 0:
            cmd.actualOutputs = map(lambda x: (x[0], x[1], os.stat(x[1]).st_size),
                                    cmd.actualOutputs)
            outputs = cmd.actualOutputs
            for x in outputs:
                self.actual[x[0]] = x[1]
        self.rFetchedFiles[token] = map(lambda x: (x[0], x[1], os.stat(x[1]).st_size),
                                self.rFetchedFiles[token])


    def resistErrno513(self, option, binPath, arglist):
        pid = None
        while pid is None:
            try:
                pid = os.spawnv(option, binPath, arglist)
            except OSError, e:
                if not (e.errno == 513):
                    raise
                pass #retry on ERESTARTNOINTR
        return pid
        
    pass # end class LocalExecutor

class RemoteExecutor:
    def __init__(self, url, slots):
        """ url: SOAP url for SWAMP slave
            slots: max number of running slots"""
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
        self.running = {}
        self.finished = {}
        self.token = 100
        self.sleepTime = 0.2
        self.actual = {}
        self.pollCache = []
        self.cmds = {}
        pass

    def busy(self):
        # soon, we should put code here to check for process finishes and
        # cache their results.
        return len(self.running) >= self.slots
    
    def launch(self, cmd, locations=[]):
        cmd.inputSrcs = locations
        log.debug("launch %s %d to %s" %(cmd.cmd,
                                         cmd.referenceLineNum, self.url))
        remoteToken = self.rpc.slaveExec(cmd.pickleNoRef())
        self.token += 1        
        self.running[self.token] = remoteToken
        self.cmds[self.token] = cmd  # save until graduation                
        return self.token

    def discard(self, token):
        assert token in self.finished
        self.finished.pop(token)
        # consider releasing files too.

    def discardFile(self, file):
        log.debug("req discard of %s on %s" %(file, self.url))
        self.actual.pop(file)
        self.rpc.discardFile(file)

    def discardFilesIfHosted(self, fileList):
        hosted = filter(lambda f: f in self.actual, fileList)
        if hosted:
            return self.discardFiles(hosted)
        

    def discardFiles(self, fileList):
        log.debug("req discard of %s on %s" %(str(fileList), self.url))
        map(self.actual.pop, fileList)
        self.rpc.discardFiles(fileList)

    def pollAny(self):
        if self.pollCache:
            return self.pollCache.pop()
        f = self.pollAll()
        if f:
            top = f.pop()
            self.pollCache += f
            return top
        return None

    def pollAll(self):
        lTokens = []
        rTokens = []
        fins = []
        for (token, rToken) in self.running.items():
            lTokens.append(token)
            rTokens.append(rToken)

        while True:
            try:
                states = self.rpc.pollStateMany(rTokens)
                break
            except socket.error, err:
                # workaround buggy python sockets: doesn't handle EINTR
                # http://bugs.python.org/issue1628205
                if err[0] == EINTR:
                    continue
                raise
            
        for i in range(len(lTokens)):
            if states[i] is not None:
                self._graduate(lTokens[i], states[i])
                fins.append((lTokens[i], states[i]))
        if fins:
            return fins
        return None
    
    def waitAny(self):
        """wait for something to happen. better be something running,
        otherwise you'll wait forever."""
        while True:
            r = self.pollAny()
            if r is not None:
                return r
            time.sleep(self.sleepTime)
        pass

    def poll(self, token):
        if token in self.finished:
            return self.finished[token]
        elif token in self.running:
            state = self._pollRemote(self.running[token])
            if state is not None:
                self._graduate(token, state)
        else:
            raise StandardError("RemoteExecutor.poll: bad token")

    def _pollRemote(self, remoteToken):
        state = self.rpc.pollState(remoteToken)
        if state is not None:
            if state != 0:
                log.error("slave %s error while executing" % self.url)
        return state # always return, whether None, 0 or nonzero

    def _addFinishOutput(self, logical, actual):
        self.actual[logical] = actual

    def _retryPollOutputs(self, token):
        while True:
            try:
                outputs = self.rpc.pollOutputs(token)
                break
            except Exception, e:
                log.error("Error in execution.py:self.rpc.pollOutputs-- error in SOAPpy/Client.py (retry in 2 seconds)")
                time.sleep(2)
                pass
    def _graduate(self, token, retcode):
        rToken = self.running.pop(token)
        cmd = self.cmds.pop(token)
        self.finished[token] = retcode
        #outputs = self.rpc.actualOuts(rToken)
        self._retryPollOutputs(rToken)
        outputs = self.rpc.pollOutputs(rToken)
        # where do i keep my commands?
        cmd.actualOutputs = []
        log.debug("adding %s from %s" % (str(outputs), str(rToken)))
        for x in outputs:
            self._addFinishOutput(x[0],x[1])
            cmd.actualOutputs.append((x[0],x[1], x[2]))

    def _waitForFinish(self, token):
        """helper function"""
        remoteToken = self.running[token]
        while True:
            state = self._pollRemote(remoteToken)
            if state is not None:
                return state
            time.sleep(self.sleepTime) # sleep for a while.  need to tune this.
        pass
        
    def join(self, token):
        if token in self.running:
            ret = self._waitForFinish(token)
            self._graduate(token, ret)
            return ret
        elif token in self.finished:
            return self.finished[token]
        else:
            raise StandardError("RemotExecutor.join: bad token")
    pass # end class RemoteExecutor 


def makeTestConfig():
    
    pass

def makeFakeExecutor():
    return NewLocalExecutor(mode='fake')

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
    
#     return LocalExecutor(NcoBinaryFinder(config),
#                              FileMapper("swamp%d"%os.getpid(),
#                                         "./s",
#                                         "./p",
#                                         "./b" ),
#                              2)

def testDispatcher():
    config = makeTestConfig()
    return testRun(config, [makeFakeExecutor()])

def testRun(config, execu):
    e = execu
    import swamp.scheduler as scheduler
    pd = scheduler.NewParallelDispatcher(config, e)
    pd.dispatchAll(loadCmds("exectestCmds.pypickle"))
    print "Running, will force stop after 20 seconds"
    try:
        time.sleep(3)
    except:
        pass
    e[0].forceJoin()
    print "cmds exec'd", pd.count

def testLocalDispatch():
    config = makeTestConfig()
    return testRun(config, [makeLocalExec(config)])
    pass
