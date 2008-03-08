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

# SWAMP imports
#from swamp.execution import NewParallelDispatcher
from swamp.partitioner import PlainPartitioner
from swamp import log

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
        self.execLocation = {} # logicalout -> executor
        #self.sleepTime = 0.100 # originally set at 0.200
        #self.running = {} # (e,etoken) -> cmd
        #self.result = None
        self.readyClusters = set() # unordered. prefer fast member testing
        self.runningClusters = set() # unordered. prefer fast member testing
        self.finishedClusters = set()
        self.count = 0
        pass

    def _dispatchRoots(self):
        unIdleExecutors = 0
        numExecutors = len(self.executors)
        while (unIdleExecutors < numExecutors) and self.rootClusters:
            e = self.polledExecutor.next()
            if e.needsWork():
                c = self.rootClusters.next()
                self._dispatchCluster(c, e)
                unIdleExecutors = 0
            else:
                unIdleExecutors += 1

    def dispatchAll(self, cmdList, hook=lambda f:None):
        # Break things into clusters:
        p = PlainPartitioner(cmdList)
        self.clusters = p.result()
        
        # Then put ready clusters in queues for each executor.
        self.rootClusters = iter(self.clusters[0])
        self.polledExecutor = itertools.cycle(self.executors)
        
        self._dispatchRoots()
        
        # remaining scheduling and dispatching will run as
        # asynchronously-initiated callbacks.

        return    


    def _registerCallback(self, cmd, gradHook, isLocal):
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
        if isLocal:
            return (lambda : self._graduate(cmd, gradHook, False),
                    lambda : self._graduate(cmd, gradHook, True))
        else:
            print "Uh oh, I don't know how to do this yet"
            self.listener.addCallback(gradHook)

            return ()

    def _unregisterCallback(self, url):
        """delete a callback.  Generally, each callback is supposed to be executed at most once.
        """
        self.listener.deleteCallback(url)
    
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
        # Discard from the running list
        self.runningClusters.discard(cluster)
        # Put cluster on a 'finished clusters' list
        self.finishedClusters.add(cluster)
        # Now update a ready clusters list-- we prefer dispatching
        # from this one over the root clusters list, thus favoring
        # depth-first rather than breadth first.
        newReady = []
        cands = filter(lambda c: (c not in self.readyClusters)
                       and (c not in self.runningClusters), cluster.children)
        print "I graduated a cluster! ", cluster
        newReady = filter(lambda c: c.ready(self.finished), cands)
        # If we made a cluster ready, dispatch it.
        if len(newReady) > 0:
            self._dispatchCluster(newReady[0], executor)
            # Then, put the remaining clusters on the ready list
            self.readyClusters.update(newReady[1:])

        elif self.readyClusters:
            # or dispatch from already-ready
            c = self.readyClusters.pop()
            self._dispatchCluster(c, executor)
        elif self.rootClusters:
            c = self.rootClusters.next()
            self._dispatchCluster(c, executor)
            
        pass

    def _dispatchCluster(self, cluster, executor):
        self.runningClusters.add(cluster)
        executor.dispatch(cluster, self._registerCallback,
                          lambda : self.graduateCluster(cluster, executor))

        
    def _graduate(self, cmd, gradHook, fail):
        #The dispatcher isn't really in charge of dependency
        #checking, so it doesn't really need to know when things
        #are finished.
        gradHook(cmd,fail) # Service the hook function first (better later?)
        print "graduate",cmd.cmd, cmd.argList, "total=",self.count
        self.count += 1
        if fail:
            print "we failed"
            # Now, process an abort.
            return
        print "succeeded"

        pass
    
        if fail:
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
            
            return
            #######
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

