# $Id$

"""
soapi - Contains core logic for swamp's SOAP interface.

 Requires twisted.web http://twistedmatrix.com/trac/wiki/TwistedWeb

"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# standard Python imports
import cPickle as pickle
import logging
import os
import threading 

# twisted imports
import twisted.web.soap as tSoap
import twisted.web.resource as tResource
import twisted.web.server as tServer
import twisted.web.static as tStatic

# SWAMP imports 
from swamp_common import *
from swamp_config import Config 


class LaunchThread(threading.Thread):
    def __init__(self, launchFunc, updateFunc):
        threading.Thread.__init__(self)
        self.launchFunc = launchFunc
        self.updateFunc = updateFunc
        
    def run(self):
        self.updateFunc(self) # put myself as placeholder
        self.updateFunc(self.launchFunc()) # update with real token

    
class JobManager:
    """JobManager manages slave tasks run on this system.
    We will want to add contexts so that different tasks do not collide in
    files, but it's not needed right now for benchmarking, and will
    complicate debugging"""
        
    def __init__(self, cfgName=None):
        if cfgName:
            self.config = Config(cfgName)
        else:
            self.config = Config()
        self.config.read()
        
        cfile = logging.FileHandler(self.config.logLocation)
        formatter = logging.Formatter('%(name)s:%(levelname)s %(message)s')
        cfile.setFormatter(formatter)
        log.addHandler(cfile)
        log.setLevel(self.config.logLevel)
        log.info("Swamp slave logging at "+self.config.logLocation)
        self.config.dumpSettings(log, logging.DEBUG)

        self.jobs = {} # dict: tokens -> jobstate
        self.fileMapper = FileMapper("slave%d"%os.getpid(),
                                     self.config.execSourcePath,
                                     self.config.execScratchPath,
                                     self.config.execBulkPath)
        self.scratchSub = "s"
        self.bulkSub = "b"
       
        self.localExec = LocalExecutor(NcoBinaryFinder(self.config),
                                       self.fileMapper)

        self.exportPrefix = "http://%s:%d/%s" % (self.config.slaveHostname,
                                                 self.config.slavePort,
                                                 self.config.slavePubPath)
        self.scratchExportPref = self.exportPrefix + self.scratchSub + "/"
        self.bulkExportPref = self.exportPrefix + self.bulkSub + "/"
        self.token = 0
        self.tokenLock = threading.Lock()
        self.publishedFuncs = [self.reset, self.slaveExec,
                               self.pollState, self.pollStateMany,
                               self.pollOutputs,
                               self.discardFile, self.discardFiles,
                               self.ping
                               ]
        self.publishedPaths = [(self.config.slavePubPath+self.scratchSub, self.config.execScratchPath),
                               (self.config.slavePubPath+self.bulkSub, self.config.execBulkPath)]
                               
        pass

    def reset(self):
        # Clean up trash from before:
        # - For now, don't worry about checking jobs still in progress
        # - Delete all the physical files we allocated in the file mapper
        log.info("Reset requested")
        self.fileMapper.cleanPhysicals()
        log.info("Reset finish")
        
    def slaveExec(self, pickledCommand):
        cf = CommandFactory(self.config)
        p = cf.unpickleCommand(pickledCommand)
        self.tokenLock.acquire()
        self.token += 1
        token = self.token + 0
        self.tokenLock.release()
        log.info("received cmd: %s %d token=%d outs=%s"
                 % (p.cmd, p.referenceLineNum, token, str(p.outputs)))
        self._threadedLaunch(p, token)
        return token

    def _updateToken(self, token, etoken):
        self.jobs[token] = etoken
        

    def _threadedLaunch(self, cmd, token):

        launch = lambda : self.localExec.launch(cmd)
        update = lambda et: self._updateToken(token, et)
        thread = LaunchThread(launch, update)
        thread.start()
        return

    def pollState(self, token):
        if token not in self.jobs:
            time.sleep(0.2) # possible race
            if token not in self.jobs:
                log.warning("token not ready after waiting.")
                return None
        if isinstance(self.jobs[token], threading.Thread):
            return None # token not even ready, arg fetch.
        res = self.localExec.poll(self.jobs[token])
        if res is not None:
            log.info("Token %d returned %s" % (token, str(res)))
            return res
        else:
            return None

    def pollStateMany(self, tokenList):
        return map(self.pollState, tokenList)

    def actualToPub(self, f):
        relative = f.split(self.config.execScratchPath + os.sep, 1)
        if len(relative) < 2:
            relative = f.split(self.config.execBulkPath + os.sep, 1)
            return self.bulkExportPref + relative[1]
        else:
            return self.scratchExportPref + relative[1]
    
    def pollOutputs(self, token):
        assert token in self.jobs
        outs = self.localExec.actualOuts(self.jobs[token])
        outs += self.localExec.fetchedSrcs(self.jobs[token])
        log.debug("outs is " + str(outs) + " for " + str(token))
        l = map(lambda t: (t[0], self.actualToPub(t[1])), outs)
        log.debug("also outs is " + str(l))
        return l


    def discardFile(self, f):
        log.debug("Discarding "+str(f))
        self.fileMapper.discardLogical(f)

    def discardFiles(self, fList):
        log.debug("Bulk discard "+str(fList))
        #for f in fList:
        for i in range(len(fList)):
            self.fileMapper.discardLogical(fList[i])
        #map(self.fileMapper.discardLogical, fList)

    def ping(self):
        return "PONG %f" %time.time()

    def listenTwisted(self):
        s = Instance((self.config.slaveHostname,
                      self.config.slavePort,
                      self.config.slaveSoapPath), 
                     self.publishedPaths,
                     self.publishedFuncs)
        s.listenTwisted()
        
    pass # end class SimpleJobManager

class Instance:
    
    def __init__(self, hostPortPath, staticPaths, funcExports):
        self.soapHost = hostPortPath[0]
        self.soapPort = hostPortPath[1]
        self.soapPath = hostPortPath[2]

        self.staticPaths = staticPaths
        self.funcExports = funcExports
        self.url = "http://%s:%d/%s" % hostPortPath

    def _makeTwistedWrapper(self, exp):
        """_makeTwistedWrapper
        -- makes an object that exports a list of functions
        exp -- a list of functions (i.e. [self.doSomething, self.reset])
        """
        class WrapperTemplate(tSoap.SOAPPublisher):
            pass
        w = WrapperTemplate()
        # construct an object to export through twisted.
        map(lambda x: setattr(w,"soap_"+x.__name__, x), exp)
        return w
        
    def listenTwisted(self):
        from twisted.internet import reactor
        root = tResource.Resource()
        tStatic.loadMimeTypes() # load from /etc/mime.types

        # setup static file paths
        map(lambda x: root.putChild(x[0],tStatic.File(x[1])),
            self.staticPaths)

        # setup exportable interface
        wrapper = self._makeTwistedWrapper(self.funcExports)
        root.putChild(self.soapPath, wrapper)

        # init listening
        reactor.listenTCP(self.soapPort, tServer.Site(root))

        log.debug("Starting worker interface at: %s"% self.url)
        reactor.run()
        pass
   


log = logging.getLogger("SWAMP")

def selfTest():
    jm = soapi.JobManager("swamp.conf")
    jm.slaveExec()

def pingTest(confFilename):
    """
    ping a server specified by a configuration file.
    """
    from twisted.web.soap import Proxy
    from twisted.internet import reactor
    import time

    c = Config(confFilename)
    c.read()
    url = "http://%s:%d/%s" % (c.slaveHostname,
                               c.slavePort,
                               c.slaveSoapPath)
    print "using url",url
    proxy = Proxy(url)
    t = time.time()
    call = proxy.callRemote('ping')

    def succ(cook):
        e = time.time()
        print "success ",cook
        a = cook.split()
        firsthalf = float(a[1]) - t
        total = e-t
        print "total time: %f firsthalf: %f" %(total, firsthalf)
        reactor.stop()
    def fail(reason):
        print "fail",reason
        reactor.stop()
    call.addCallbacks(succ, fail)
    reactor.run()
    
    
