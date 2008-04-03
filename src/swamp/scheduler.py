# $Id: $
"""
   scheduler -- a module containing logic for scheduling commands
   or perhaps clusters, for dispatch and execution.
"""

# 
# Copyright (c) 2007 Daniel Wang, Charles S. Zender
# This source file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)
#

# Python dependencies:
import cPickle as pickle
import itertools
import md5
import struct
import time
import threading

# SWAMP imports
#from swamp.execution import NewParallelDispatcher
from swamp.partitioner import PlainPartitioner
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


class Scheduler:
    def __init__(self, config, graduateHook=lambda x: None):
        self.config = config
        self.env = {}
        self.taskId = self._makeTaskId()
        self.cmdList = []
        self.cmdsFinished = []
        self.fileLocations = {}
        self._graduateHook = graduateHook
        self.result = None # result is True on success, or a string/other-non-boolean on failure.
        self.cmdCount = 0
        self.serFinish = []
        pass
                           

    def _makeTaskId(self):
        # As SWAMP matures, we should rethink the purpose of a taskid
        # It's used now to disambiguate different tasks in the database
        # and to provide a longer-lived way to reference a specific task.

        # If we just need to disambiguate, just get some entropy.
        # System time should be enough entropy
        digest = md5.md5(str(time.time())).digest()
        # take first 4 bytes, convert to hex, strip off 0x and L
        ## assume int is 4 bytes. works on dirt (32bit) and tephra(64bit)
        assert struct.calcsize("I") == 4 
        taskid = hex(struct.unpack("I",digest[:4])[0])[2:10] 
        return taskid
    

    def schedule(self, parserCommand):
        """Accept a command for scheduling"""
        parserCommand.schedNum(self.cmdCount)
        self.cmdList.append(parserCommand)
        self.cmdCount += 1 # 
        pass

    def finish(self):
        """tell the scheduler that there are no more commands forthcoming"""
        #if self.transaction != None:
        #    self.transaction.finish()
        pass
    
    def graduateHook(self, hook):
        """hook is a unary void function that accepts a graduating command
        as a parameter.  Invocation occurs sometime after the command
        commits its output file to disk."""
        self._graduateHook = hook
        pass

    def _graduateAction(self, cmd):
        # save file size data.
        #cmd.actualOutputs = map(lambda x: (x[0],x[1], os.stat(x[1]).st_size),
        #                        cmd.actualOutputs)
        #print "produced outputs:", cmd.actualOutputs
        self.cmdsFinished.append(cmd)
        #print "scheduler finished cmd", cmd
        return self._graduateHook(cmd)

    
    def executeSerialAll(self, executor=None):
    
        def run(cmd):
            if executor:
                tok = executor.launch(cmd)
                retcode = executor.join(tok)
                return retcode
        for c in self.cmdList:
            ret = run(c)
            if ret != 0:
                log.debug( "ret was "+str(ret))
                log.error("error running command %s" % (c))
                self.result = "ret was %s, error running %s" %(str(ret), c)
                break
            

    def executeParallelAll(self, executors=None):
        """executors is a container(currently, a list) of the available
        executors for this job.
        In the future, we would like to allow this container to grow/contract,
        and in other words, allow run-time add/remove of executors.
        """
        if not executors:
            log.error("Missing executor for parallel execution. Skipping.")
            return
        #self.pd = ParallelDispatcher(self.config, executors)
        self.pd = NewParallelDispatcher(self.config, executors)
        self.fileLocations = self.pd.dispatchAll(self.cmdList,
                                                 self._graduateAction)
        self.result = self.pd.result
        pass
    pass # end of class Scheduler

class NewParallelDispatcher:
    def __init__(self, config, executorList):
        self.config = config
        self.executors = executorList
        self.finished = set()
        self.okayToReap = True
        # not okay to declare file death until everything is parsed.
        self.execLocation = {} # logicalout -> [(executor,url),...]
        #self.sleepTime = 0.100 # originally set at 0.200
        #self.running = {} # (e,etoken) -> cmd
        #self.result = None
        self.readyClusters = set() # unordered. prefer fast member testing
        self.qClusters = set() # unordered. prefer fast member testing
        self.finishedClusters = set()
        self.gradLock = threading.Lock()
        self.stateLock = threading.Lock()
        self.count = 0
        self.targetCount = 0
        self.result = None
        self.resultEvent = threading.Event()
        self.listener = config.callback
        pass

    def _dispatchRoots(self):
        unIdleExecutors = 0
        numExecutors = len(self.executors)
        while (unIdleExecutors < numExecutors) and self.rootClusters:
            e = self.polledExecutor.next()
            if e.needsWork():
                self.stateLock.acquire()
                c = self.rootClusters.next()
                self._dispatchCluster(c, e)
                self.stateLock.release()
                unIdleExecutors = 0
            else:
                unIdleExecutors += 1

    def dispatchAll(self, cmdList, hook=lambda f:None):
        self.resultEvent.clear()
        self.gradHook = hook
        self.targetCount += len(cmdList)
        # Break things into clusters:
        p = PlainPartitioner(cmdList)
        self.clusters = p.result()
        
        # Then put ready clusters in queues for each executor.
        self.rootClusters = iter(self.clusters[0])
        self.polledExecutor = itertools.cycle(self.executors)
        
        self._dispatchRoots()
        
        # remaining scheduling and dispatching will run as
        # asynchronously-initiated callbacks.

        # caller expects us to block until finish, so let's block.
        self.resultEvent.wait()
        return    


    def _registerCallback(self, cmd, gradHook, executor, isLocal):
        """Used by mgmt "threads" that dispatch clusters/cmds remotely.
        Returns the callback url.
        1. Build data struct determining how to process result.
        2. Register the callback with this function.
        3. Dispatch the cmd/cluster to the remote system.
        4. join.  The async callback will handle results processing.

        Client probably wants to register both ACK and NACK.  Both
        entries should be unregistered when either gets called, unless
        more advanced handling is desired.
        Alternatively, we can let the thread babysit the remote
        operation and detect faults.  
        """
        funcs = [lambda custom: self._graduate(cmd, gradHook, executor, False, custom),
                 lambda custom: self._graduate(cmd, gradHook, executor, True, custom)]
        if isLocal:
            return funcs
        else:
            urls = map(self.config.callback.registerEvent, funcs)
            print "made callback urls %s" % str(urls)
            return urls

    def _unregisterCallback(self, url):
        """delete a callback.  Generally, each callback is supposed to be executed at most once.
        """
        self.listener.deleteCallback(url)

    def idle(self):
        return self.count >= self.targetCount

    def keepBusy(self):
        # run threads for each executor?... well, no.
        # We'll run a thread for each thread in a local executor
        # And we'll register callbacks for remote executors.
        # When we receive a callback, we'll just process it.
        # (not sure if parallelism suffers.)
        # 
        # periodically wakeup and check status.  Here, we can insert
        # code for status-checking: can detect faults in remote/local exec.
        while not self.done:
            sleep(0.3)
        return
    
    def graduateCluster(self, cluster, executor):
        ## Much of this graduation logic should be moved to the other
        ## 'graduate' function.  That will let child clusters start
        ## earlier, notably when they depend on a subset of the
        ## cluster's outputs
        rp = ""
        # Put cluster on a 'finished clusters' list
        self.finishedClusters.add(cluster)
        # Now update a ready clusters list-- we prefer dispatching
        # from this one over the root clusters list, thus favoring
        # depth-first rather than breadth first.
        newReady = []
        self.gradLock.acquire()
        ckag = map(id, self.readyClusters) + map(id, self.qClusters)
        ckag.sort()
        
        cands = filter(lambda c: (c not in self.readyClusters)
                       and (c not in self.qClusters),
                       cluster.children)
        print "am I idle?", self.idle(), self.targetCount, self.count
        rp += "I graduated a cluster! " + str(id(cluster)) + "\n"
        newReady = filter(lambda c: c.ready(self.finished), cands)
        # If we made a cluster ready, dispatch it.

        c = None # Buffer to minimize lock holding time
        if len(newReady) > 0:
            c = newReady[0]
            # Then, put the remaining clusters on the ready list
            self.readyClusters.update(newReady[1:])
            rp +=  "clusFrom parent: %s\n" % str(id(cluster))
        elif self.readyClusters:
            # or dispatch from already-ready
            c = self.readyClusters.pop()
            rp +=  "clusFrom readylist\n"
        elif self.rootClusters:
            c = self.rootClusters.next()
            rp +=  "clusFrom ROOT\n"
        if c:
            self.qClusters.add(c) # Ugly.
        self.gradLock.release()
        if c: 
            self._dispatchCluster(c, executor)


        if self.idle():
            self.result = True 
            self.resultEvent.set()
        pass

    def _dispatchCluster(self, cluster, executor):
        # need to put code here to find input files and pass their locs forward.
        inputs = set()
        map(lambda i: inputs.update(i),
            map(lambda c: c.inputsWithParents, cluster.roots))
        inputLocs = map(lambda f:(f, self.fileLoc(f)), inputs)
        #print "cluster parents are",map(id,inputs)
        #print "cluster's cluster parents are", map(id,cluster.parents)
        executor.dispatch(cluster, self._registerCallback,
                          lambda : self.graduateCluster(cluster, executor),
                          outputPatch=self.config.serverUrlFromFile,
                          locations=inputLocs)

        
    def _graduate(self, cmd, gradHook, executor, fail, custom):
        #The dispatcher isn't really in charge of dependency
        #checking, so it doesn't really need to know when things
        #are finished.
        gradHook(cmd, fail, custom) # Service the hook function first (better later?)
        # this is the executor's hook
        #print "graduate",cmd.cmd, cmd.argList, "total=",self.count, fail
        self.count += 1
        if fail:
            origline = ' '.join([cmd.cmd] + map(lambda t: ' '.join(t), cmd.argList) + cmd.leftover)
            s = "Bad return code %s from cmdline %s %d outs=%s" % (
                "", origline, cmd.referenceLineNum, str(cmd.outputs))
            log.error(s)
            # For nicer handling, we should find the original command line
            # and pass it back as the failing line (+ line number)
            # It would be nice to trap the stderr for that command, but that
            # can be done later, since it's a different set of pipes
            # to connect.

            self.result = "Error at line %d : %s" %(cmd.referenceLineNum,
                                                    origline)
            self.resultEvent.set()
            return
            #raise StandardError(s)
        else:
            # figure out which one finished, and graduate it.
            #self.finished[cmd] = code
            log.debug("graduating %s %d" %(cmd.cmd,
                                           cmd.referenceLineNum))
            print "graduating %s %d %s" %(cmd.cmd,
                                       cmd.referenceLineNum,
                                       id(cmd))
            self.finished.add(cmd)
            #print "New Finished set:", len(self.finished),"\n","\n".join(map(lambda x:x.original,self.finished))
            # Are any clusters made ready?
            # Check this cluster's descendents.  For each of them,
            # see if the all their parent cmds are finished.
            # For now, don't dispatch a cluster until all its parents
            # are ready.

            # If it's a leaf cmd, then publish its results.
            # Apply reaper logic: should be same as before.

            # delete consumed files.
            if self.okayToReap:
                self.releaseFiles(filter(lambda f: self.isDead(f, cmd),
                                         cmd.inputsWithParents))
            e = executor # token is (executor, etoken)
            map(lambda o: appendList(self.execLocation, o[0], (executor,o[1])),
                cmd.actualOutputs)
            

            self.gradHook(cmd)
            if self.idle():
                self.result = True 
                self.resultEvent.set()
            return

    def isDead(self, file, consumingCmd):
        # Find the producing cmd, see if all its children are finished.
        return 0 == len(filter(lambda c: c not in self.finished,
                               consumingCmd.factory.commandByLogicalIn[file]))

    def releaseFiles(self, files):
        if not files:
            return
        log.debug("ready to delete " + str(files)+ "from " + str(self.executors))
        # collect by executors
        map(lambda e: e.discardFilesIfHosted(files), self.executors)

        try:
            # cleanup execLocation
            map(self.execLocation.pop, files)
        except:
            print "error execlocationpop",files
            pass

   
    def fileLoc(self, file):
        """ return url of logical output file"""
        execs = self.execLocation[file]
        if execs:
            return execs[0][1] # Return first location's url.
        else:
            return None
