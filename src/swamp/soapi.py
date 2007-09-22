# $Id$

"""
soapi - Contains core logic for swamp's SOAP interface.

 Requires twisted.web http://twistedmatrix.com/trac/wiki/TwistedWeb

"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# standard Python imports
import logging

# twisted imports
import twisted.web.soap as tSoap
import twisted.web.resource as tResource
import twisted.web.server as tServer
import twisted.web.static as tStatic

# SWAMP imports
from swamp import log

class Instance:
    
    def __init__(self, hostPortPath, staticPaths,
                 funcExports, customChildren):
        self.soapHost = hostPortPath[0]
        self.soapPort = hostPortPath[1]
        self.soapPath = hostPortPath[2]

        self.staticPaths = staticPaths
        self.funcExports = funcExports
        self.customChildren = customChildren
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

        
        map(lambda x: root.putChild(x[0],x[1]), self.customChildren)

        # init listening
        reactor.listenTCP(self.soapPort, tServer.Site(root))

        log.debug("Starting worker interface at: %s"% self.url)
        reactor.run()
        pass
   

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
    
    
