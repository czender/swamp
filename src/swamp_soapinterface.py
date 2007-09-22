#!/usr/bin/env python
# $Id$
# $URL$
#
# This file is released under the GNU General Public License version 3 (GPLv3)
# Copyright (c) 2007 Daniel L. Wang

# SWAMP imports
from swamp_common import *
from swamp_config import Config
from swamp_transact import *
from swamp import log
import swamp.soapi as soapi
import swamp.inspector as inspector

# Standard Python imports
import cPickle as pickle
import logging
import os
import threading

# (semi-) third-party imports
import SOAPpy
import twisted.web.soap as tSoap
import twisted.web.resource as tResource
import twisted.web.server as tServer
import twisted.web.static as tStatic

# SWAMP imports

import swamp
swamp.SoapInterfaceVersion = "$Id$"


class LaunchThread(threading.Thread):

    def __init__(self, swampint, script, filemap, updateFunc):
        threading.Thread.__init__(self) 
        self.script = script
        self.swampInterface = swampint
        self.updateFunc = updateFunc
        self.filemap = filemap
        pass

    def run(self):
        self.updateFunc(self) # put myself as placeholder
        log.info("Starting workflow execution")
        task = self.swampInterface.submit(self.script, self.filemap)
        log.info("Admitted workflow: workflow id=%s" % task.taskId())
        # should update with an object that can be used to
        #query for task state.
        self.updateFunc(task) # update with real object

class StandardJobManager:
    """StandardJobManager manages submitted tasks dispatched by this system.
    It *should* only apply the presentation semantics and interface
    layer, and should not manage queuing logic.
    """
    def __init__(self, cfgName=None):
        if cfgName:
            config = Config(cfgName)
        else:
            config = Config()
        config.read()
        le = LocalExecutor.newInstance(self.config)
        self.filemap = FileMapper("f"+str(os.getpid()),
                                  config.execSourcePath,
                                  config.execResultPath,
                                  config.execResultPath)
        self.resultExportPref = "http://%s:%d/%s/" % (config.serverHostname,
                                                      config.serverPort,
                                                      config.serverFilePath)
        self.resultExportPref = self.exportPrefix + "/"

        self.publishedPaths = [(self.config.serverFilePath,
                                self.config.execResultPath)]
        self.publishedFuncs = [self.reset,
                               self.newScriptedFlow, self.discardFlow,
                               self.pollState, self.pollOutputs,
                               self.pollJob,
                               self.pyInterface]

        self.swampInterface = SwampInterface(config, le)
        self.config = config
        self._setupVariablePreload(self.swampInterface)
        self.swampInterface.startLoop()
        self.token = 0
        self.tokenLock = threading.Lock()
        self.jobs = {}
        self.discardedJobs = {}
        pass

    def _setupVariablePreload(self, interface):
        interface.updateVariablePreload({
            "SWAMPVERSION" : "0.1+",
            "SHELL" : "swamp",
            "SWAMPHOSTNAME" : self.config.serverHostname,
            })
        return

    def reset(self):
        # Clean up trash from before:
        # - For now, don't worry about checking jobs still in progress
        # - Delete all the physical files we allocated in the file mapper
        if self.config.serverMode == "production":
            log.info("refusing to do hard reset: unsafe for production")
            return
        log.info("Reset requested--disabled")
        #self.fileMapper.cleanPhysicals()
        log.info("Reset finish")
        
    def newScriptedFlow(self, script):
        self.tokenLock.acquire()
        self.token += 1
        token = self.token + 0
        self.tokenLock.release()
        log.info("Received new workflow (%d) {%s}" % (token, script))
        self._threadedLaunch(script, token)
        log.debug("return from thread launch (%d)" % (token))
        return token

    def pyInterface(self, cmdline):
        """pyInterface(cmdline) : runs an arbitrary python commmand
        line and returns its results.  This is a huge security hole that
        should be disabled for live systems.

        It's very handy during development, though."""

        if self.config.serverMode != "debug":
            return "Error, debugging is disabled."

        try:
            return str(eval(cmdline))
        except Exception, e:
            import traceback, sys
            tb_list = traceback.format_exception(*sys.exc_info())
            return "".join(tb_list)
        pass
        
    def _updateToken(self, token, etoken):
        self.jobs[token] = etoken
        
    def _threadedLaunch(self, script, token):
        launchthread = LaunchThread(self.swampInterface, script,
                                    self.filemap,
                                    lambda x: self._updateToken(token, x))
        launchthread.start()
        log.debug("started launch")
        #launchthread.join()
        return 

    def taskStateObject(self, task):
        token = -1 # use dummy token for now.
        if isinstance(task, threading.Thread):
            return SwampTaskState.newState(token, "submitted")
        if isinstance(task, SwampTask):
            r = task.result()
            if r == True:
                return SwampTaskState.newState(token, "finished")
            elif r != None:
                return SwampTaskState.newState(token, "generic error",r)
            else:
                # is the task running?
                pos = self.swampInterface.queuePosition(task)
                if pos >= 0:
                    if pos == 0:
                        msg = "Queued: Next in line"
                    elif pos > 0:
                        msg = "Queued: %d ahead in line" % pos
                    return SwampTaskState.newState(token, "waiting", msg)
                extra = task.status()
                return SwampTaskState.newState(token, "running",extra)
        return None

    def pollState(self, token):
        if token not in self.jobs:
            time.sleep(0.2) # possible race
            if token not in self.jobs:
                log.warning("token not ready after waiting.")
                return SwampTaskState.newState(token, "missing").packed()
        stateObject = self.taskStateObject(self.jobs[token])
        stateObject.token = token
        if not stateObject:
            log.error("SOAP interface found weird object in self.jobs:" +
                      "token(%d) has %s" %(token, str(self.jobs[token])) )
            return SwampTaskState.newState(token, "system error").packed()
        else:
            return stateObject.packed()

    def pollStateMany(self, tokenList):
        return map(self.pollState, tokenList)

    def pollJob(self, token):
        """poll a job, using a job token"""
        if isinstance(self.jobs[token], SwampTask):
            task = self.jobs[token]
            
            r = task.result()
            if r == True:
                return [0,""]
            elif r != None:
                return [1, r]
        else:
            return None
        



    def actualToPub(self, f):
        log.debug("++"+f +self.config.execResultPath)
        relative = f.split(self.config.execResultPath + os.sep, 1)
        if len(relative) < 2:
            log.info("Got request for %s which is not available"%f)
            return self.resultExportPref
        else:
            return self.resultExportPref + relative[1]
    
    def pollOutputs(self, token):
        assert token in self.jobs
        task = self.jobs[token]
        outs = task.scrAndLogOuts
        outUrls = map(lambda f: (f[0], self.actualToPub( # make url from file
            task.outMap.mapReadFile(f[1]))), # find output localfile
                       outs) #start from logical outs.

        log.debug("polloutputs: outs "+str(outs))
        log.debug("polloutputs: outUrls "+str(outUrls))

        return outUrls

    def discardFile(self, f):
        log.debug("Discarding "+str(f))
        self.fileMapper.discardLogical(f)

    def discardFiles(self, fList):
        log.debug("Bulk discard "+str(fList))
        #for f in fList:
        for i in range(len(fList)):
            self.fileMapper.discardLogical(fList[i])
        #map(self.fileMapper.discardLogical, fList)

    def discardFlow(self, token):
        task = self.jobs[token]
        task.outMap.cleanPhysicals()
        self.discardedJobs[token] = self.jobs.pop(token)
        log.debug("discarding for token %d" %(token))
        pass

    def startTwistedServer(self):
        self.config.serverInspectPath = "inspect"
        custom = [("inspect", inspector.newResource(self.config))]
        self.config.runtimeJobManager = self
        s = soapi.Instance((self.config.serverHostname,
                            self.config.serverPort,
                            self.config.serverPath), 
                           self.publishedPaths,
                           self.publishedFuncs,
                           custom)
        s.listenTwisted()
        return 
        

    def grimReap(self):
        self.swampInterface.grimReap()

    def registerWorker(self, certificate, offer):
        # for now, accept all certificates.
        log.debug("Received offer from %s with %d slots" %(offer))
        (workerUrl, workerSlots) = offer
        result = self.swampInterface.addWorker(url, slots)
        if not result:
            log.error("Error registering worker " + url)
            return False
        return True
                
    pass # end class StandardJobManager


def selfTest():
    pass

def clientTest():
    import SOAPpy
    serverConf = Config("swampsoap.conf")
    serverConf.read()
    server = SOAPpy.SOAPProxy("http://localhost:%d/%s"
                              %(serverConf.serverPort,
                                serverConf.serverPath))
    if len(sys.argv) > 2:
        import readline
        while True:
            print server.pyInterface(raw_input())
    else:
        server.reset()
        tok = server.newScriptedFlow("""
ncwa -a time -dtime,0,3 camsom1pdf/camsom1pdf_10_clm.nc timeavg.nc
ncwa -a lon timeavg.nc timelonavg.nc
ncwa -a time -dtime,0,2 camsom1pdf/camsom1pdf_10_clm.nc timeavg.nc


    """)
        print "submitted, got token: ", tok
        while True:
            ret = server.pollState(tok)
            if ret is not None:
                print "finish, code ", ret
                break
            time.sleep(1)
        outUrls = server.pollOutputs(tok)
        print "actual outs are at", outUrls
        for u in outUrls:
            # simple fetch, since we are single-threaded.
            urllib.urlretrieve(u[1], u[0])
        



def main():
    selfTest()
    if (len(sys.argv) > 1) and (sys.argv[1] == "--"):
        clientTest()
    else:
        jm = StandardJobManager("swamp.conf")
        #jm.startSlaveServer()
        jm.startTwistedServer()
        jm.grimReap() # necessary to wakeup and kill threads.

if __name__ == '__main__':
    main()

