# $Header: /cvsroot/nco/nco/src/ssdap/swamp_soapslave.py,v 1.10 2007/05/18 00:46:26 wangd Exp $
# Copyright (c) 2007 Daniel L. Wang
from swamp_common import *
from swamp_config import Config 
import cPickle as pickle
import logging
import os
import SOAPpy
import threading 
import twisted.web.soap as tSoap
import twisted.web.resource as tResource
import twisted.web.server as tServer
import twisted.web.static as tStatic

log = logging.getLogger("SWAMP")

class LaunchThread(threading.Thread):
    def __init__(self, executor, cmd, updateFunc):
        threading.Thread.__init__(self) 
        self.executor = executor
        self.cmd = cmd
        self.updateFunc = updateFunc
        pass
    def run(self):
        self.updateFunc(self) # put myself as placeholder
        etoken = self.executor.launch(self.cmd)
        self.updateFunc(etoken) # update with real token

    
class SimpleJobManager:
    """SimpleJobManager manages slave tasks run on this system.
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
        launchthread = LaunchThread(self.localExec, cmd,
                                    lambda et: self._updateToken(token, et))
        launchthread.start()
        #launchthread.join()
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
        return map(lambda t: (t[0], self.actualToPub(t[1])), outs)
        #map(lambda f: (f,self.actualToPub(f)), outs) 
        #return outs

    def discardFile(self, f):
        log.debug("Discarding "+str(f))
        self.fileMapper.discardLogical(f)

    def discardFiles(self, fList):
        log.debug("Bulk discard "+str(fList))
        #for f in fList:
        for i in range(len(fList)):
            self.fileMapper.discardLogical(fList[i])
        #map(self.fileMapper.discardLogical, fList)

    def startSlaveServer(self):
        #SOAPpy.Config.debug =1
    
        server = SOAPpy.SOAPServer(("localhost", self.config.slavePort))
        server.registerFunction(self.slaveExec)
        server.registerFunction(self.pollState)
        server.registerFunction(self.pollStateMany)
        server.registerFunction(self.pollOutputs)
        server.registerFunction(self.reset)
        server.registerFunction(self.discardFile)
        server.registerFunction(self.discardFiles)
        server.serve_forever()
        pass

    def startTwistedSlaveServer(self):
        from twisted.internet import reactor
        root = tResource.Resource()
        scratchFileRes = tStatic.File(self.config.execScratchPath)
        bulkFileRes = tStatic.File(self.config.execBulkPath)
        tStatic.loadMimeTypes() # load from /etc/mime.types
        root.putChild(self.config.slavePubPath+self.scratchSub, scratchFileRes)
        root.putChild(self.config.slavePubPath+self.bulkSub, bulkFileRes)
        root.putChild(self.config.slaveSoapPath, TwistedSoapWrapper(self))
        reactor.listenTCP(self.config.slavePort, tServer.Site(root))
        log.debug("starting Twisted soap slave")
        reactor.run()
        pass
    pass # end class SimpleJobManager

class TwistedSoapWrapper(tSoap.SOAPPublisher):
    def __init__(self, jobManager):
        self.jobManager = jobManager
    def soap_reset(self):
        return self.jobManager.reset()
    def soap_slaveExec(self, pickled):
        return self.jobManager.slaveExec(pickled)
    def soap_pollState(self, token):
        return self.jobManager.pollState(token)
    def soap_pollStateMany(self, tokenList):
        return self.jobManager.pollStateMany(tokenList)
    def soap_pollOutputs(self, token):
        return self.jobManager.pollOutputs(token)
    def soap_discardFile(self, file):
        return self.jobManager.discardFile(file)
    def soap_discardFiles(self, file):
        return self.jobManager.discardFiles(file)





def fakeCommand():
    cf = CommandFactory(Config.dummyConfig())
    ins = ["camsom1pdf/camsom1pdf.cam2.h1.0001-01-01-00000.nc"]
    outs = ["out.nc"]
    linenum = 20
    c = cf.newCommand("ncwa", ({},[],ins+outs),(ins,outs), linenum)
    return c.pickleNoRef()


def selfTest():
    pass

def clientTest():
    import SOAPpy
    #server = SOAPpy.SOAPProxy("http://localhost:8080/SOAP")
    server = SOAPpy.SOAPProxy("http://localhost:8080")
    server.reset()
    tok = server.slaveExec(fakeCommand())
    print "submitted, got token: ", tok
    while True:
        ret = server.pollState(tok)
        if ret is not None:
            print "finish, code ", ret
            break
        time.sleep(1)
    print "actual outs are at", server.pollOutputs(tok)

def main():
    selfTest()
    
    jm = SimpleJobManager("slave.conf")
    #jm.startSlaveServer()
    jm.startTwistedSlaveServer()

if __name__ == '__main__':
    main()

