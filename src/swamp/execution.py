# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
execution - contains code related to executing commands


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import itertools
import os
import time
import threading
from heapq import * # for minheap implementation

# for working around python bug http://bugs.python.org/issue1628205
import socket
from errno import EINTR

# third party imports
from SOAPpy import SOAPProxy

# swamp imports
from swamp.mapper import FileMapper # just for LocalExecutor.newInstance
from swamp import log




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
        log.debug("dispatching cmdlist="+str(cmdList))
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
        log.debug("clearing to make room for writing (should be using concretefiles")
        self.clearFiles(map(lambda t: t[1], cmd.actualOutputs))
        pid = self.resistErrno513(os.P_NOWAIT,
                                  self.binaryFinder(cmd), cmdLine)
        log.debug("child pid: "+str(pid))

        self.running[token] = pid
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
                if (pid2 != 0) and os.WIFEXITED(status):
                    code = os.WEXITSTATUS(status)
                    self._graduate(token, code)
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
            pid = self.running.pop(token)
            (pid, status) = os.waitpid(pid,0)
            
            log.debug("got "+str(pid)+" " +str(status)+"after spawning")
            if os.WIFEXITED(status):
                status = os.WEXITSTATUS(status)
            else:
                status = -1
            self.finished[token] = status
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
        log.debug("finish token %d with code %d" %(token,retcode))
        self.finished[token] = retcode
        self.running.pop(token)

        cmd = self.cmds[token]
        cmd.actualOutputs = map(lambda x: (x[0], x[1], os.stat(x[1]).st_size),
                                cmd.actualOutputs)
        outputs = cmd.actualOutputs

        for x in outputs:
            self.actual[x[0]] = x[1]


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

    def _graduate(self, token, retcode):
        rToken = self.running.pop(token)
        cmd = self.cmds.pop(token)
        self.finished[token] = retcode
        #outputs = self.rpc.actualOuts(rToken)
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

