#!/usr/bin/env python
# $Id$
# $URL$
#
# This file is released under the GNU General Public License version 3 (GPLv3)
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender

# SWAMP imports
from swamp_common import *
from swamp.config import Config
from swamp_transact import *
from swamp import log
import swamp.soapi as soapi
import swamp.inspector as inspector
from swamp.execution import LocalExecutor
from swamp.mapper import FileMapper

# Standard Python imports
import cPickle as pickle
import getopt
from itertools import izip,imap
from operator import itemgetter
import logging
import os
import sys
import thread
import threading

# (semi-) third-party imports
import SOAPpy
import twisted.web.resource as tResource

# SWAMP imports

import swamp
swamp.SoapInterfaceVersion = "$Id$"

class LaunchThread(threading.Thread):
    def __init__(self, launchFunc, updateFunc):
        threading.Thread.__init__(self)
        self.launchFunc = launchFunc
        self.updateFunc = updateFunc
        
    def run(self):
        self.updateFunc(self) # put myself as placeholder
        self.updateFunc(self.launchFunc()) # update with real token

class MoveMeCallbackResource(tResource.Resource):
    # should inherit twisted.web.resource.Resource
    # if we override init, we need to explicitly call parent constructor
    def __init__(self, config, subPath):
        tResource.Resource.__init__(self)
        self.config = config
        self.urlTable = {}
        self.prefix = "http://%s:%d/%s/" %(config.serviceHostname,
                                           config.servicePort, subPath)

        config.callback = self # put myself in the config as callback
        pass
    def getChild(self, path, request):
        # getchild is necessary to use numbered syntax.
        return self
    def render_GET(self, request):
        # here, we should check the handler to do the right thing.
        key = request.prepath[1]
        url = self.prefix + key
        #if key in self.urlTable:
        #    return self.urlTable[key]
        try:
            self.urlTable[url](request.args) # Perform the callback.
        except KeyError:
            return "<html>Unsuccessful call <br/>prepath %s <br/> postpath %s<br/> args %s</html> "% (request.prepath, request.postpath, request.args)
        return """<html>
      Hello, world! I am located at %r. and you requested %s
      Urltable has %s
    </html>""" % (request.prepath, key, str(self.urlTable))
    def registerEvent(self, func):
        # Assign a url. Do we care that this exposes the object ID?
        key = str(id(func)) 
        url = self.prefix + key
        self.urlTable[url] = func
        return url
    def unregisterEvent(self, url):
        try:
            self.urlTable.remove(url)
        except:
            log.error("Tried to remove non-registered callback %s" % url)
        pass
        

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
        le = LocalExecutor.newInstance(config) #always make one
        self.filemap = FileMapper("f"+str(os.getpid()),
                                  config.execSourcePath,
                                  config.execResultPath,
                                  config.execResultPath)

        self.publishedPaths = [(config.servicePubPath,
                                config.execResultPath),
                               (config.servicePubPath + "s",
                                config.execScratchPath),
                               (config.servicePubPath + "b",
                                config.execBulkPath)]
        self.exportTemplate = lambda s: "http://%s:%d/%s/" % (config.serviceHostname,
                                                              config.servicePort,
                                                              s)
        self.exportPrefix = map( self.exportTemplate, 
                                 map(itemgetter(0), self.publishedPaths))

        self.publishedFuncs = [self.reset,
                               self.newScriptedFlow, self.discardFlow,
                               self.pollState, self.pollOutputs,
                               #self.pollJob,
                               self.pyInterface,
                               self.registerWorker,
                               self.unregisterWorker]

        self.swampInterface = SwampInterface(config, le)
        self.config = config
        self._setupVariablePreload(self.swampInterface)
        self.swampInterface.startLoop()
        self.token = 0
        self.tokenLock = threading.Lock()
        self.jobs = {}
        self.discardedJobs = {}
        self._setupMaster()
        pass

    def _setupMaster(self):
        self._workers = {}
        self._nextWorkerToken = 1
        self.config.serverUrlFromFile = self._actualToPub


    def _setupVariablePreload(self, interface):
        interface.updateVariablePreload({
            "SWAMPVERSION" : "0.1+",
            "SHELL" : "swamp",
            "SWAMPHOSTNAME" : self.config.serviceHostname,
            })
        return

    def reset(self):
        # Clean up trash from before:
        # - For now, don't worry about checking jobs still in progress
        # - Delete all the physical files we allocated in the file mapper
        if self.config.serviceLevel == "production":
            log.info("refusing to do hard reset: unsafe for production")
            return
        assert self.config.serviceLevel in ["debug","testing"]
        log.info("Reset requested--disabled")
        #self.fileMapper.cleanPhysicals()
        log.info("Reset finish")
        
    def newScriptedFlow(self, script):
        self.tokenLock.acquire()
        self.token += 1
        token = self.token + 0
        self.tokenLock.release()
        #log.info("Received new workflow (%d) {%s}" % (token, script))
        log.info("Received new workflow (%d) {%s}" % (token, ""))
        self._threadedLaunch(script, token)
        log.debug("return from thread launch (%d)" % (token))
        return token

    def pyInterface(self, cmdline):
        """pyInterface(cmdline) : runs an arbitrary python commmand
        line and returns its results.  This is a huge security hole that
        should be disabled for live systems.

        It's very handy during development, though."""

        if self.config.serviceLevel != "debug":
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

    def _launchScript(self, script, token):
        self._updateToken(token, thread.get_ident()) # put in a placeholder
        log.info("Admitting workflow for execution")
        task = self.swampInterface.submit(script, self.filemap)
        log.info("Admitted workflow: workflow id=%s" % task.taskId())
        self._updateToken(token, task)
        return task
        
    def _threadedLaunch(self, script, token):
        """this is pretty easy to merge: just think about the right
        point to parameterize."""
        launch = lambda : self._launchScript(script, token)
        update = lambda x: x
        thread = LaunchThread(launch, update)
        thread.start()
        return 

    def _taskStateObject(self, task):
        token = -1 # use dummy token for now.
        if isinstance(task, int):
            return SwampTaskState.newState(token, "submitted")
        if isinstance(task, SwampTask):
            r = task.result()
            if r == True:
                s = statistics.tracker().scriptStat(task.taskId())
                extra = s.statList()
                return SwampTaskState.newState(token, "finished", extra)
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
        """ this can be merged soon"""
        if token not in self.jobs:
            time.sleep(0.2) # possible race
            if token not in self.jobs:
                log.warning("token not ready after waiting.")
                if self.config.serviceMode == "master":
                    return SwampTaskState.newState(token, "missing").packed()
                else:
                    return None
        if self.config.serviceMode != "master":
            log.error("pollState not implemented here yet")
        stateObject = self._taskStateObject(self.jobs[token])
        stateObject.token = token
        if not stateObject:
            log.error("SOAP interface found weird object in self.jobs:" +
                      "token(%d) has %s" %(token, str(self.jobs[token])) )
            return SwampTaskState.newState(token, "system error").packed()
        else:
            return stateObject.packed()

    def pollStats(self, token):
        if token not in self.jobs:
            time.sleep(0.2) # possible race
            if token not in self.jobs:
                log.warning("token not ready after waiting.") 
        
                

    def _actualToPub(self, f):

        for ((ppath, ipath),pref) in izip(self.publishedPaths,
                                          self.exportPrefix):
            relative = f.split(ipath + os.sep, 1)
            if len(relative) >= 2:
                return pref + relative[1]
            
        log.info("Got request for %s which is not available"%f)
        return self.exportPrefix[0]

    
    def pollOutputs(self, token):
        assert token in self.jobs
        task = self.jobs[token]
        outs = task.scrAndLogOuts
        outUrls = map(lambda f: (f[0], self._actualToPub( # make url from file
            task.outMap.mapReadFile(f[1]))), # find output localfile
                       outs) #start from logical outs.

        return outUrls


    def discardFlow(self, token):
        task = self.jobs[token]
        task.cleanPhysicals()
        self.discardedJobs[token] = self.jobs.pop(token)
        log.debug("discarding for token %d" %(token))
        pass


    def startTwistedServer(self):
        self.config.serviceInspectPath = "inspect"
        custom = [("inspect", inspector.newResource(self.config)),
                  ("worker", MoveMeCallbackResource(self.config, 'worker'))]
        self.config.runtimeJobManager = self
        s = soapi.Instance((self.config.serviceHostname,
                            self.config.servicePort,
                            self.config.serviceSoapPath,
                            self.config.serviceXmlPath), 
                           self.publishedPaths,
                           self.publishedFuncs,
                           custom)
        s.listenTwisted()
        return 
        

    def grimReap(self):
        self.swampInterface.grimReap()

    def registerWorker(self, certificate, offer):
        # for now, accept all certificates.
        log.debug("Received offer from %s with %d slots" %(offer[0],offer[1]))
        (workerUrl, workerSlots) = (offer[0], offer[1])
        result = self.swampInterface.addWorker(workerUrl, workerSlots)
        token = self._nextWorkerToken
        self._nextWorkerToken += 1
        self._workers[token] = result

        if not result:
            log.error("Error registering worker " + url)
            return None
        return token

    def unregisterWorker(self, sessionToken):
        if sessionToken not in self._workers:
            return None
        self.swampInterface.dropWorker(self._workers[sessionToken])
        return True
                
    pass # end class StandardJobManager


def selfTest():
    pass

def clientTest():
    import SOAPpy
    serverConf = Config("swampsoap.conf")
    serverConf.read()
    server = SOAPpy.SOAPProxy("http://localhost:%d/%s"
                              %(serverConf.servicePort,
                                serverConf.serviceSoapPath))
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
        return

    confname = "swamp.conf"
    # Split this config handling to something scalable
    # if once we have >2 options
    (opts, leftover) = getopt.getopt(sys.argv[1:], "c:",["config="])
    offeredConfs = []
    for o in opts:
        if (o[0] == "-c") or (o[0] == "--config"):
            offeredConfs.append(o[1])
    if offeredConfs:
        confname = offeredConfs

    jm = StandardJobManager(confname)
    jm.startTwistedServer()
    jm.grimReap() # necessary to wakeup and kill threads.

if __name__ == '__main__':
    main()

