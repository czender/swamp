# $Id$

"""
server - Contains 'top-level' code for swamp server instances


"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import os
import socket
import sys
import threading 

# (semi-) third-party module imports
import SOAPpy

# SWAMP imports 
from swamp import log
from swamp_common import *
from swamp.config import Config     
import swamp.inspector as inspector
import swamp.soapi as soapi
from swamp.mapper import FileMapper

# FIXME: reorg code to not need these two:
from swamp.execution import LocalExecutor
from swamp.execution import NcoBinaryFinder


class LaunchThread(threading.Thread):
    def __init__(self, launchFunc, updateFunc):
        threading.Thread.__init__(self)
        self.launchFunc = launchFunc
        self.updateFunc = updateFunc
        
    def run(self):
        self.updateFunc(self) # put myself as placeholder
        self.updateFunc(self.launchFunc()) # update with real token


class WorkerConnector(threading.Thread):
    def __init__(self, prepFunc, target, offer):
        """
        prepFunc: is a function that returns True/False.  Run before
          making a connection, and do not attempt connection if it
          returns False.  This is used to do last-minute hostname resolution.
        target is a tuple: (url, certificate)
          url: A string containing the SOAP url for the master
          certificate: An opaque object to present to the master.  Masters
          should only accept registrations from certificates they trust.
        offer is a void function returning: (url, slots)
          url: A string containing the SOAP url for the offering interface
          slots: Scheduling slot count
          (consider providing CPU and I/O metrics, as well as a catalog
        """
        threading.Thread.__init__(self)
        self._target = target
        self._offer = offer
        self._prepFunc = prepFunc
        self.active = True
        self._connected = False
        self._timeStart = time.time()
        self._timeLastAttempt = None
        self.exitJustification = None
        self.timeBetweenAttempts = 5 # 30 seconds
        self.timeToGiveUp = 1800 # 30 minutes
        self.maxSleep = 2
        
    def run(self):
        log.debug("Start registration attempt")
        while self.active:
            # maintain a connection
            if (not self._connected) and (not self._timeout()):
                self._connect()
            else:
                time.sleep(5)
        if self._connected:
            self._tryDisconnect() # attempt to gracefully disconnect
        pass

    def _timeout(self):
        """return True if we have timed out."""
        # Timer is reset at:
        # a) construction/initialization,
        # b) server disconnects.
        if (time.time() - self._timeStart) >= self.timeToGiveUp:
            self.active = False
            self.exitJustification = "Timeout: giving up after %d seconds" % (
                self.timeToGiveUp)
            return True
        else:
            pass
            #print time.time()-self._timeStart, " seconds is not timed out"
        return False
    
    def _connect(self):
        """attempt a connection, if we've waited long enough."""
        if not self._timeLastAttempt:
            self._tryConnect()
        else:
            waittime =  time.time() - self._timeLastAttempt
            if waittime >= self.timeBetweenAttempts:
                self._tryConnect()
            else:
                sys.stderr.write(".")
                remaining = self.timeBetweenAttempts - waittime
                if remaining > self.maxSleep:
                    time.sleep(self.maxSleep)
                else:
                    time.sleep(remaining)
                
        pass 

    def _tryDisconnect(self):
        server = SOAPpy.SOAPProxy(self._target[0])
        # logging may have been closed--e.g. Ctrl-C initiated 
        #log.debug("Disconnecting")
        ack = server.unregisterWorker(self._token)
        #log.debug("Disconnected from master")
        pass
    
    def _tryConnect(self):
        """actually, make an attempt at connecting."""
        server = SOAPpy.SOAPProxy(self._target[0])
        # make some soap
        # connect and register my url and slot count.
        # in the future, register my catalog
        try:
            prepresult = self._prepFunc()
            if not prepresult:
                ack = False
            else:
                log.debug("Connecting to %s -- %s" % (
                    str(time.ctime()), self._target)) 
                ack = server.registerWorker(self._target[1], self._offer())
        except:
            ack = False
        if not ack: # or some other failure mode
            # Characterize failure
            # fail- do not try again
            # fail- try again later
            # timeout - try again later
            log.debug("Bad registration, will retry " + str(ack))
            self._timeLastAttempt = time.time()
            return
        log.debug("Successfuly registered to " + self._target[0])
        self._token = ack
        # on success, set connected.
        self._connected = True
        
    
class JobManager:
    """JobManager manages slave tasks run on this system.
    We will want to add contexts so that different tasks do not collide in
    files, but it's not needed right now for benchmarking, and will
    complicate debugging"""
        
    def __init__(self, cfgName, overrides):
        if cfgName:
            self.config = Config(cfgName)
        else:
            self.config = Config()
        self.config.read()
        self.config.update(overrides)
        
        self.config.dumpSettings(log, logging.DEBUG)
        self._setupLogging(self.config)
        
        self.jobs = {} # dict: tokens -> jobstate
   
        self.token = 0
        self.tokenLock = threading.Lock()
        self._modeSetup("worker")
                               
        pass

    def _modeSetup(self, mode):
        if mode == "worker":
            return self._setupWorker()
            pass
        elif mode == "master":
            log.error("Frontend/master code not migrated yet..")
            pass
        else:
            log.error("Invalid server mode, don't know what to do.")
            print 'Panic! Invalid server mode, expecting "worker" or "master"'
            return

    def _setupWorker(self):
        self.fileMapper = FileMapper("slave%d"%os.getpid(),
                                     self.config.execSourcePath,
                                     self.config.execScratchPath,
                                     self.config.execBulkPath)
        self.localExec = LocalExecutor(NcoBinaryFinder(self.config),
                                       self.fileMapper)
        
        self._adjustHostnameFields()
        target = (self.config.masterUrl, self.config.masterAuth)
        offer = lambda : (self.soapUrl, self.config.execLocalSlots)
        self.registerThread = WorkerConnector(self._fixHostname, target, offer)
        self.lateInit = self.registerThread.start
        pass

    def _setupMaster(self):
        pass
    

    def _fixHostname(self):
        # auto-determine hostname?
        if self.config.serviceHostname == "<auto>":
            name = self._checkHostname(self.config.masterUrl)
            if name:
                self.config.serviceHostname = name
                self._adjustHostnameFields()
            else:
                log.debug("Can't resolve own hostname: is master running?")
                return False
        return True # hostname is okay, no need to fix.
        
        

    def _checkHostname(self, target):
        """
        Check our own ip address by opening a TCP connection to our
        target url (probably the master's url) and seeing which interface
        we used.
        """
        #split out hostname and port from url.
        typeTuple = urllib.splittype(target)
        assert typeTuple[0] == "http"
        (host, port) = urllib.splitport(urllib.splithost(typeTuple[1])[0])
        if not port:
            port = 80
        else:
            port = int(port) 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            #print "trying to determine outside hostname", host, port
            s.connect((host, port)) #make the connection
            #print "connect...."
            (ip, port) = s.getsockname()
            result = socket.gethostbyaddr(ip)[0]
            log.debug("My hostname is " + result)
        except Exception, e: # On any exception, give up.
            #log.debug("<auto> hostname resolution failed")
            result = None
        s.close()
        return result

    def _adjustHostnameFields(self):
        # prefixes for remapping.
        scratchSub = "s"
        bulkSub = "b"
        self.exportPrefix = "http://%s:%d/%s" % (self.config.serviceHostname,
                                                 self.config.servicePort,
                                                 self.config.servicePubPath)
        self.scratchExportPref = self.exportPrefix + scratchSub + "/"
        self.bulkExportPref = self.exportPrefix + bulkSub + "/"
        self.soapUrl = "http://%s:%d/%s" % (self.config.serviceHostname,
                                            self.config.servicePort,
                                            self.config.serviceSoapPath)

        self.publishedFuncs = [self.reset, self.slaveExec,
                               self.pollState, self.pollStateMany,
                               self.pollOutputs,
                               self.discardFile, self.discardFiles,
                               self.ping
                               ]
        self.publishedPaths = [(self.config.servicePubPath + scratchSub,
                                self.config.execScratchPath),
                               (self.config.servicePubPath + bulkSub,
                                self.config.execBulkPath)]
        
        
    def _setupLogging(self, config):
        cfile = logging.FileHandler(config.logLocation)
        formatter = logging.Formatter('%(name)s:%(levelname)s %(message)s')
        cfile.setFormatter(formatter)
        log.addHandler(cfile)
        log.setLevel(config.logLevel)
        log.info("Swamp slave logging at " + config.logLocation)
        

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
        #log.debug("actual is " + str(outs) + " for " + str(token))
        outs += self.localExec.fetchedSrcs(self.jobs[token])
        #log.debug("outs is " + str(outs) + " for " + str(token))
        l = map(lambda t: (t[0], self.actualToPub(t[1]), t[2]), outs)
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

    def dangerousRestart(self, auth):
        # Think about what sort of checks we should do to prevent orphan
        # processes and resources. FIXME.
        args = sys.argv #take the original arguments
        args.insert(0, sys.executable) # add python
        os.execv(sys.executable, args) # replace self with new python.

    def grimReap(self):
        self.registerThread.active = False

    def listenTwisted(self):
        self.config.serviceInspectPath = "inspect"
        custom = [("inspect", inspector.newResource(self.config))]
        s = soapi.Instance((self.config.serviceHostname,
                            self.config.servicePort,
                            self.config.serviceSoapPath), 
                           self.publishedPaths,
                           self.publishedFuncs,
                           custom)
        s.listenTwisted(self.lateInit)

    
        
    pass # end class JobManager
 
def selfTest():
    pass

def pingServer(configFilename):
    soapi.pingTest(configFilename)

def startServer(configFilename, overrides={}):
    selfTest()

    #jm = soapi.JobManager(configFilename)
    
    jm = JobManager(configFilename, overrides)
    jm.listenTwisted()
    jm.grimReap()
    pass

