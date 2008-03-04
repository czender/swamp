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
import md5
import struct
import time

# SWAMP imports
#from swamp.execution import NewParallelDispatcher
from swamp.partitioner import PlainPartitioner

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
        #self.finished = {}
        self.okayToReap = True
        # not okay to declare file death until everything is parsed.
        self.execLocation = {} # logicalout -> executor
        #self.sleepTime = 0.100 # originally set at 0.200
        #self.running = {} # (e,etoken) -> cmd
        #self.execPollRR = None
        #self.execDispatchRR = None
        #self.result = None
        self.count = 0
        pass

    def _dispatchRoots(self):
        unIdleExecutors = 0
        numExecutors = len(self.executors)
        while (unIdleExecutors < numExecutors) and self.rootClusters:
            e = self.polledExecutor.next()
            if e.needsWork():
                e.dispatch(self.rootClusters.next(), self._registerCallback)
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
    def dispatchAll(self, cmdList, hook=lambda f:None):
        # Break things into clusters:
        p = PlainPartitioner(cmdList)
        self.clusters = p.result() # reference copy (is this needed?)
        self.remainingClusters = map(lambda c: c[:], self.clusters) 
        print "partitions=",self.clusters

        readyclusters = self.remainingClusters[0]
        # The lower levels (i.e. [1], [2], etc.) get executed as their
        # parents complete.
        
        readyclusters.reverse()
        # Then put ready clusters in queues for each executor.
        for e in self.executors:
            if not readyclusters:
                break
            e.enqueue(readyclusters.pop())
        # Now make sure the executors are busy with work.
        self.keepBusy()


    def _registerCallback(self, function, isLocal):
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
            return (lambda : self._graduate(cmd, False),
                    lambda : self._graduate(cmd, True))
        else:
            print "Uh oh, I don't know how to do this yet"
            self.listener.addCallback(function)

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
    
        

    def _graduate(self, cmd, fail):
        print "graduate",cmd.cmd, cmd.argList
        self.count += 1
        if fail:
            print "we failed"
            # Now, process an abort.
            return
        print "succeeded"

        pass


