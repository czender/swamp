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
import twisted.web.xmlrpc as tXmlrpc
import twisted.web.resource as tResource
import twisted.web.server as tServer
import twisted.web.static as tStatic

# SWAMP imports
from swamp import log
from swamp import subproc

class Instance:
    
    def __init__(self, hostPortPath, staticPaths,
                 funcExports, customChildren):
        self.soapHost = hostPortPath[0]
        self.soapPort = hostPortPath[1]
        self.soapPath = hostPortPath[2]
        if (len(hostPortPath) > 3) and (len(hostPortPath[3]) > 0):
                self.xmlPath = hostPortPath[3]
        else:
            self.xmlPath = None

        self.staticPaths = staticPaths
        self.funcExports = funcExports
        self.customChildren = customChildren
        self.url = "http://%s:%d/%s" % hostPortPath[:3]

    def _makeWrapper(self, exp, prefix, wrapperClass):
        """_makeWrapper: makes an object that exports a list of functions
        exp:   a list of functions (e.g. [self.doSomething, self.reset])
        prefix:       name prefix
        wrapperClass: parent of new class
        """
        class Wrapper(wrapperClass):
            pass
        w = Wrapper()
        # construct an object to export through twisted.
        map(lambda x: setattr(w, "_".join([prefix,x.__name__]), x), exp)
        return w
        
        
    def listenTwisted(self, extInit=lambda : None):
        from twisted.internet import reactor
        root = tResource.Resource()
        tStatic.loadMimeTypes() # load from /etc/mime.types

        # setup static file paths
        map(lambda x: root.putChild(x[0],tStatic.File(x[1])),
            self.staticPaths)

        # setup exportable interface
        print "publish",self.soapPath
        root.putChild(self.soapPath, self._makeWrapper(self.funcExports,
                                                      "soap",
                                                      tSoap.SOAPPublisher))
        if self.xmlPath:
            print "publish",self.xmlPath
            root.putChild(self.xmlPath, self._makeWrapper(self.funcExports,
                                                          "xmlrpc",
                                                          tXmlrpc.XMLRPC))
        
        map(lambda x: root.putChild(x[0],x[1]), self.customChildren)

        # init listening
        reactor.listenTCP(self.soapPort, tServer.Site(root))

        log.debug("Starting SWAMP interface at: %s"% self.url)
        print "Starting SWAMP interface at: %s"% self.url
        extInit()
        subproc.usingTwisted = True
        reactor.run()
        pass
   


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
    
    
