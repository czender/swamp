
# $Id$
# swamp_common.py - a module containing the parser and scheduler for SWAMP
#  not meant to be used standalone.
# 
# Copyright (c) 2007, Daniel L. Wang and Charles S. Zender
# Licensed under the GNU General Public License v3

"""Parser and scheduler module for SWAMP
This provides high level access to SWAMP parsing and scheduling functionality.
"""
__author__ = "Daniel L. Wang <wangd@uci.edu>"
#__all__ = ["Parser"] # I should fill this in when I understand it better
SwampCoreVersion = "$Id$"

# Python imports
import cPickle as pickle
import copy # for shallow object copies for pickling
#import getopt
import logging
import md5
import os
import re
import shutil
import struct
import subprocess
import time
import threading
import urllib

import  profile


# SWAMP imports
#
from swamp.config import Config
from swamp.parser import Parser
from swamp.command import CommandFactory
from swamp import log
from swamp.execution import ParallelDispatcher
from swamp.execution import RemoteExecutor
from swamp.mapper import LinkedMap
import swamp.statistics as statistics

def isRemote(filepath):
    return filepath.startswith("http://")        

class Scheduler:
    def __init__(self, config, graduateHook=lambda x: None):
        self.config = config
        self.transaction = None
        self.env = {}
        self.taskId = self.makeTaskId()
        self.cmdList = []
        self.cmdsFinished = []
        self.fileLocations = {}
        self._graduateHook = graduateHook
        self.result = None # result is True on success, or a string/other-non-boolean on failure.
        self.cmdCount = 0
        pass

    def makeTaskId(self):
        # As SWAMP matures, we should rethink the purpose of a taskid
        # It's used now to disambiguate different tasks in the database
        # and to provide a longer-lived way to reference a specific task.

        # if we just need to disambiguate, just get some entropy.
        # this should get us enough entropy
        digest = md5.md5(str(time.time())).digest()
        # take first 4 bytes, convert to hex, strip off 0x and L
        ## assume int is 4 bytes. works on dirt (32bit) and tephra(64bit)
        assert struct.calcsize("I") == 4 
        taskid = hex(struct.unpack("I",digest[:4])[0])[2:10] 
        return taskid
    
    def instanceJobPersistence(self):
        """finds the class's instance of a JobPersistence object,
        creating if necessary if it doesn't exist, and caching for
        future use."""
        if self.env.has_key("JobPersistence"):
            o = self.env["JobPersistence"] 
            if o != None:
                return o
        o = JobPersistence(self.config.dbFilename, True)
        self.env["JobPersistence"] = o
        return o


    def initTransaction(self):
        log.debug("FIXME: tried to init db transaction.")
        assert self.transaction is None
        jp = self.instanceJobPersistence()
        trans = jp.newPopulationTransaction()
        self.persistedTask = trans.insertTask(self.taskId)
        assert self.persistedTask is not None
        self.transaction = trans
        pass
    
    def schedule(self, parserCommand):
        if False and self.transaction is None:
            self.initTransaction()
        if False:
            self.transaction.insertCmd(parserCommand.referenceLineNum,
                                       parserCommand.cmd, parserCommand.original)
            #concrete = logical # defer concrete mapping
            def insert(f, isOutput):
                self.transaction.insertInOutDefer(parserCommand.referenceLineNum,
                                                  f, f, isOutput, 1)
                pass
            map(lambda f: insert(f, False), parserCommand.inputs)
            map(lambda f: insert(f, True), parserCommand.outputs)
            pass
        parserCommand.schedNum(self.cmdCount)
        self.cmdList.append(parserCommand)
        self.cmdCount += 1
        pass

    def finish(self):
        if self.transaction != None:
            self.transaction.finish()
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
        if not executors:
            log.error("Missing executor for parallel execution. Skipping.")
            return
        self.pd = ParallelDispatcher(self.config, executors)
        self.fileLocations = self.pd.dispatchAll(self.cmdList,
                                                 self._graduateAction)
        self.result = self.pd.result
        pass
    pass # end of class Scheduler

            


class SwampTask:
    """Contains objects necessary to manage *ONE* script's running context"""
    def __init__(self, remote, config, script, outMap,
                 customizer=lambda p,s,cf: True):
        """
    remote -- a list of executors where jobs can be sent.
    config -- a configuration object, e.g. the global config object
    script -- the script to be executed
    outMap -- a filemap object to be used to map between logical and
    physical filenames

    customizer -- a function accepting a parser instance, scheduler
    instance, and a command factory instance.  This function will be
    called after construction (but before parsing) to apply any
    customization desired or necessary for an environment.
    """
        self.config = config
        self.parser = Parser()
        #FIXME: need to define publishIfOutput
        self.scheduler = Scheduler(config, self._publishIfOutput)
        self.parser.commandHandler(self.scheduler.schedule)
        self._commandFactory = CommandFactory(self.config)
        self.buildTime = time.time()
        self.fail = None
        if not customizer(self.parser,
                          self.scheduler,
                          self._commandFactory):
            self.fail = "Error applying frontend customization."
            return

        self.remoteExec = remote
        self.outMap = LinkedMap(outMap, self.taskId())
        self._publishedFiles = []
        self.stat = statistics.tracker().scriptStart((self.taskId(), script, self))
        if True: # set to False to profile the parse step
            self._parseScript(script)
            self.stat.markParseFinish()
            return
        try:
            pobject = profile.Profile()
            presult = pobject.runctx('self._parseScript(script)', globals(),locals())
            presult.dump_stats("/home/wangd/parse.pyprofile")
        except StandardError, e:
            self.fail = str((e,e.__doc__, str(e)))
            raise e
        self.stat.markParseFinish()
        pass

    def _parseScript(self, script):
        log.debug("Starting parse")
        self.parser.parseScript(script, self._commandFactory)
        log.debug("Finish parse")
        self.scheduler.finish()
        log.debug("finish scheduler prep")
        self.scrAndLogOuts = self._commandFactory.realOuts()
        self.logOuts = map(lambda x: x[1], self.scrAndLogOuts)
        log.debug("outs are " + str(self.scrAndLogOuts))
        pass

    def _publishIfOutput(self, obj):
        """object can be either a logical filename or a command,
        or a list of either"""
        if getattr(obj, "actualOutputs", False): # duck-typing
            actfiles = obj.actualOutputs
        else:
            log.debug("publishifoutput expected cmd, but got %s"%str(obj))
            #don't know how to publish.
            pass
        log.debug("publishHook: obj is " + str(obj))
        log.debug("raw outs are %s" %(str(actfiles)))
        files = filter(lambda f: f[0] in self.logOuts, actfiles)
        log.debug("filtered is %s" %(str(files)))
        targetfiles = map(lambda ft: (ft[0], ft[1],
                                      self.outMap.mapWriteFile(ft[0])),
                          files)        
        # fork a thread for this in the future.
        self._publishHelper(targetfiles)


    def _publishHelper(self, filetuples):
        for t in filetuples:
            log.debug("publish " + str(t))
            actual = t[1]
            target = t[2]
            if isRemote(actual):
                #download, then add to local map (in db?)
                #log.debug("Download start %s -> %s" % (actual, target))
                urllib.urlretrieve(actual, target)
                #Don't forget to discard.
                log.debug("Fetch-published "+actual)
            else: #it's local!
                # this will break if we request a read on the file
                #
                # remove the file from the old mapping (discard)
                log.debug("start file move")
                shutil.move(actual, target)
                # FIXME: ping the new mapper so that it's aware of the file.
                log.debug("Published " + actual)
            self._publishedFiles.append((t[0], os.stat(target).st_size))

        pass
    def taskId(self):
        return self.scheduler.taskId
    def run(self):
        if not self.result():
            log.debug("Starting parallel dispatcher")
            self.scheduler.executeParallelAll(self.remoteExec)
        else: # refuse to run if we have a failure logged.
            log.debug("refusing to dispatch: " + str(self.result()))
            pass
        self.stat.stop()
        self.stat.outputFiles(self._publishedFiles)
        self.stat.inputFiles(map(lambda x: (x,
                                            os.stat(self.config.execSourcePath
                                                    +"/"+x).st_size),
                                 self._commandFactory.scriptIns))
        self.stat.commandList(self.scheduler.cmdList)
        self.stat.finish()
        pass

    def result(self):
        if self.fail:
            return self.fail
        else:
            return self.scheduler.result

    def status(self):
        s = self.scheduler
        result = {"executedCount" : len(s.cmdsFinished),
                  "commandCount" : len(s.cmdList)}
        return result

    def cleanPhysicals(self):
        self.outMap.cleanPhysicals()

    def selfDestruct(self):
        log.debug("Task %d is self-destructing" %(str(self.scheduler.taskId)))
        
        
class SwampInterface:
    """SwampInterface is a class which exports an interface to doing the meat
    of swamp:  task admission and top level execution managment.  Its looping
    thread runs whatever tasks are queued up."""
    def __init__(self, config, executor=None):
        self.config = config
        #setup logger
        cfile = logging.FileHandler(self.config.logLocation)
        formatter = logging.Formatter('%(name)s:%(levelname)s %(message)s')
        cfile.setFormatter(formatter)
        log.addHandler(cfile)
        log.setLevel(self.config.logLevel)
        log.info("Swamp master logging at " + self.config.logLocation)
        self.config.dumpSettings(log, logging.DEBUG)

        statistics.initTracker(config)

        if executor:
            self.defaultExecutor = executor
        else:
            self.defaultExecutor = FakeExecutor()

        if config.execSlaveNodes > 0:
            remote = []
            for i in range(config.execSlaveNodes):
                s = config.slave[i]
                remote.append(RemoteExecutor(s[0], s[1]))
            self.executor = remote
        else:
            self.executor = [self.defaultExecutor]
        self.mainThread = SwampInterface.MainThread(self)
        self.variablePreload = {}
        pass

    class MainThread(threading.Thread):
        def __init__(self, interface):
            threading.Thread.__init__(self)
            self.interface = interface
            self.freeTaskCondition = threading.Condition()
            self.ready = []
            self.running = None
            self.done = []
            self.markedForDeath = False
            pass

        def run(self):
            # Consume one item
            while True:
                self.freeTaskCondition.acquire()
                while not self.ready:
                    if self.markedForDeath:
                        self.freeTaskCondition.release()
                        return
                    self.freeTaskCondition.wait()
                    pass
                self.runReadyTask()
                self.freeTaskCondition.release()
                pass
            pass
        def acceptTask(self, task):
            # Produce one item
            self.freeTaskCondition.acquire()
            self.ready.append(task)
            self.freeTaskCondition.notify()
            self.freeTaskCondition.release()

        def acceptDeath(self):
            self.freeTaskCondition.acquire()
            self.markedForDeath = True
            self.freeTaskCondition.notify()
            self.freeTaskCondition.release()
            
        def runReadyTask(self):
            """run the first ready task at the head of the list.
            precondition:  Task list is locked (i.e. condition is acquired)
                           There exists at least one ready job
            postcondition: Task list is locked (i.e. condition is acquired)
                           One job has been run from the top of the list.
                           """
            # move top of readylist to running
            assert self.ready
            log.debug("running one task")

            self.running = self.ready.pop(0)
            # release lock!
            self.freeTaskCondition.release()
            # execute
            self.running.run()
            # re-acquire lock to log termination
            self.freeTaskCondition.acquire()
            # move running to done.
            self.done.append(self.running)
            self.running = None            

        def queuePos(self, task):
            """Check the position of a task in the queue.
            'task' should be a SwampTask object.

            @return an integer representing the count of jobs ahead
            of it in the queue, or -1 if it is not on the queue
            """
            try:
                return self.ready.index(task)
            except ValueError:
                return -1
            
        pass # end of class MainThread

    def startLoop(self):
        self.mainThread.start() 
        pass

    def grimReap(self):
        """Added this to wakeup the waiting loop thread and kill it because
        it won't die otherwise.  Not needed in test code, so it's probably
        because of the twisted.reactor environment, which traps the
        keyboard interrupt and merely aborts the reactor's listening."""
        self.mainThread.acceptDeath()

    def submit(self, script, outputMapper):
        t = SwampTask(self.executor, self.config,
                      script, outputMapper, self._customizer)
        log.info("after parse: " + time.ctime())
        self.mainThread.acceptTask(t)
        return t

    def addWorker(self, url, slots):
        log.debug("trying to add new worker: %s with %d" %(url,slots))
        re = RemoteExecutor(url,slots)
        self.executor.append(re)
        assert re in self.executor
        return re

    def dropWorker(self, executor):
        if executor in self.executor:
            log.info("Removing worker " + executor.url )
            self.executor.remove(executor)
            return True
        else:
            log.warning("Tried, but couldn't remove " + executor.url)
            return True
        
    def fileStatus(self, logicalname):
        """return state of file by name"""

        # go check db for state
        jp = JobPersistence(self.config.dbFilename)
        poll = jp.newPollingTransaction()
        i = poll.pollFileStateByLogical(logicalname)
        jp.close()
        if i is not None:
            return i
        else:
            return -32768
        pass


    def taskFileStatus(self, taskid):
        state = 0
        logname = "dummy.nc"
        url = "NA"
        # go check db for state
        jp = JobPersistence(self.config.dbFilename)
        poll = jp.newPollingTransaction()
        i = poll.pollFileStateByTaskId(taskid)
        jp.close()
        if i is not None:
            return i
        else:
            return -32768
        pass

        # want to return list of files + state + url if available
        return [(state, logname, url)]

    def queuePosition(self, task):
        return self.mainThread.queuePos(task)
    
    def execSummary(self):
        return (self.mainThread.running,
                self.mainThread.ready,
                self.mainThread.done)

    def updateVariablePreload(self, newVars):
        self.variablePreload.update(newVars)
        return 

    def _customizer(self, parser, scheduler, commandFactory):
        if len(self.variablePreload) > 0:
            parser.updateVariables(self.variablePreload)
        return True

    pass




class ActionBag:
    """ActionBag contains Threads.  The bag's containment allows it
        to peform cleanup routines as needed.  Threads are reclaimed
        as soon as they are free-- do not expect thread.local data to
        live after Thread.run terminates."""

    class Thread(threading.Thread):
        """ActionBag.Thread is a wrapper class to wrap up a function
        call so that it can be executed in a non-blocking manner. A
        callback is used to signify completion.
        
        An ActionBag.Thread is attached to an ActionBag, """
        def __init__(self, action, callback):
            self.action = action
            self.callback = callback
            pass

        def run(self):
            rv = self.action()
            self.callback(rv)

    def __init__(self):
        self.running = []        
        pass
    
    def executeAction(self, action, callback):
        t = ActionBag.Thread(action, callback)
        t.start()
        self.running.append(t)
        pass
    
    def houseclean(self):
        # iterate through running threads and reclaim if done
        # for now, this means getting rid of dead threads in the list
        self.running = filter(lambda t: t.isAlive(), self.running)
            
    pass



######################################################################
######################################################################
testScript4 = """!/usr/bin/env bash
ncrcat -O camsom1pdf/camsom1pdf.cam2.h1.0001*.nc camsom1pdf_00010101_00011231.nc # pack the year into a single series
 ncap -O -s "loctime[time,lon]=float(0.001+time+(lon/360.0))" -s "mask2[time,lon]=byte(ceil(0.006000-abs(loctime-floor(loctime)-0.25)))" camsom1pdf_00010101_00011231.nc yrm_0001am.nc
 ncwa -O --rdd -v WINDSPD -a time -d time,0,143 -B "mask2 == 1" yrm_0001am.nc yr_0001d0_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d0_am.nc.deleteme yr_0001d0_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,144,287 -B "mask2 == 1" yrm_0001am.nc yr_0001d1_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d1_am.nc.deleteme yr_0001d1_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,288,431 -B "mask2 == 1" yrm_0001am.nc yr_0001d2_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d2_am.nc.deleteme yr_0001d2_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,432,575 -B "mask2 == 1" yrm_0001am.nc yr_0001d3_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d3_am.nc.deleteme yr_0001d3_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,576,719 -B "mask2 == 1" yrm_0001am.nc yr_0001d4_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d4_am.nc.deleteme yr_0001d4_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,720,863 -B "mask2 == 1" yrm_0001am.nc yr_0001d5_am.nc.deleteme
ncap -O -h -s "time=0.25+floor(time)" yr_0001d5_am.nc.deleteme yr_0001d5_am.nc

ncwa -O --rdd -v WINDSPD -a time -d time,864,1007 -B "mask2 == 1" yrm_0001am.nc yr_0001d6_am.nc.deleteme

ncrcat -O camsom1pdf/camsom1pdf.cam2.h1.0002*.nc camsom1pdf_00020101_00021231.nc # pack the year into a single series
 ncap -O -s "loctime[time,lon]=float(0.001+time+(lon/360.0))" -s "mask2[time,lon]=byte(ceil(0.006000-abs(loctime-floor(loctime)-0.25)))" camsom1pdf_00020101_00021231.nc yrm_0002am.nc
"""

        
def testParser():
    test1 = """#!/usr/local/bin/bash
# Analysis script for CAM/CLM output.
#export CASEID1=camsomBC_1998d11
ncwa in.nc out.nc
CASEID1=$1
DATA=nothing
export ANLDIR=${DATA}/anl_${CASEID1}
export STB_YR=00
export FRST_YR=0000
export LAST_YR=0014

export MDL=clm2
let Y1=$FRST_YR+1
let Y9=1
let Y9=1+1

mkdir -p ${ANLDIR} #inline comment
mkdir -p ${DATA}/${CASEID1}/tmp

# Move /tmp files into original directories (strictly precautionary):
mv ${DATA}/${CASEID1}/tmp/* ${DATA}/${CASEID1}/

# Move data from year 0 out of the way
mv ${DATA}/${CASEID1}/${CASEID1}.${MDL}.h0.${FRST_YR}-??.nc ${DATA}/${CASEID1}/tmp/



# STEP 1: Create ensemble annual and seasonal means

# Bring in December of year 0 for seasonal mean
mv ${DATA}/${CASEID1}/tmp/${CASEID1}.${MDL}.h0.${FRST_YR}-12.nc ${DATA}/${CASEID1}/

for yr in `seq $Y1 $LAST_YR`; do
    YY=`printf "%04d" ${yr}`
    let yrm=yr-1
    YM=`printf "%04d" ${yrm}`
    ncrcat -O ${DATA}/${CASEID1}/${CASEID1}.${MDL}.h0.${YM}-12.nc ${DATA}/${CASEID1}/${CASEID1}.${MDL}.h0.${YY}-??.nc ${ANLDIR}/foo2.nc

"""
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d%b%Y %H:%M:%S')
#                    filename='/tmp/myapp.log',
#                    filemode='w')
    p = Parser()
    p.parseScript(test1, None)

profref = None

def testParser2():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d%b%Y %H:%M:%S')
    wholelist = open("full_resamp.swamp").readlines()
    test = [ "".join(wholelist[:10]),
             "".join(wholelist[:400]),
             "".join(wholelist),
             testScript4]
    
    p = Parser()
    cf = CommandFactory(Config.dummyConfig())
    p.parseScript(test[1], cf)


def testParser3():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d%b%Y %H:%M:%S')

    p = Parser()
    portionlist = ["ncwa in.nc temp.nc", "ncwa temp.nc temp.nc",
                   "ncwa temp.nc out.nc"]
    portion = "\n".join(portionlist)
    cf = CommandFactory(Config.dummyConfig())
    p.parseScript(portion, cf)
 

def testSwampInterface():
    from swamp.execution import LocalExecutor
    from swamp.execution import FakeExecutor
    #logging.basicConfig(level=logging.DEBUG)
    wholelist = open("full_resamp.swamp").readlines()
    test = [ "".join(wholelist[:10]),
             "".join(wholelist[:6000]),
             "".join(wholelist),
             testScript4]

    c = Config("swamp.conf")
    c.read()
    fe = FakeExecutor()
    le = LocalExecutor.newInstance(c)
    #si = SwampInterface(fe)
    si = SwampInterface(c, le)
    log.info("after configread at " + time.ctime())

    #evilly force the interface to use a remote executor
    assert len(si.remote) > 0
    si.executor = si.remote[0]
    taskid = si.submit(test[1])
    log.info("finish at " + time.ctime())
    print "submitted with taskid=", taskid
def main():
    #testParser3()
    #testExpand()
    testSwampInterface()

if __name__ == '__main__':
    main()

#timing results:
#
# case 1: 1.2Ghz Pentium M laptop, 512MB,
# complete full_resamp parse/submit w/command-line building
# swamp_common.py,v 1.6
#
# real    2m54.891s
# user    2m52.979s
# sys     0m1.724s
