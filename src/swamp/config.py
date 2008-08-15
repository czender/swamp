# $Id$

"""
config - Contains core logic for handling SWAMP configuration.
"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# Standard Python imports
import ConfigParser
import logging
import os
import sys

from swamp import log

class Config:
    """class Config: contains our local configuration information
    settings get written as attributes to an instance"""

    #defaults
    CFGMAP = [("logDisable", "log", "disable", False),
              ("logLocation", "log", "location",
               "/home/wangd/opendap/iroot/ssdap.log"),
              ("logLevel", "log", "level", logging.DEBUG),

              ("execNco4", "exec", "nco4",
               "/home/wangd/opendap/iroot/nco_ncdf4_bin"),
              ("execNcoDap", "exec", "ncodap",
               "/home/wangd/opendap/iroot/nco_dap_bin"),
              ("execScratchPath", "exec", "scratchPath", "."),
              ("execResultPath", "exec", "resultPath", "."),
              ("execSourcePath", "exec", "sourcePath", "."),
              ("execBulkPath", "exec", "bulkPath", "."),
              

              ("execLocalSlots", "exec", "localSlots", 2),
              ("execSlaveNodes", "exec", "slaveNodes", 0),

              ("serviceHostname", "service", "hostname", "localhost"),
              ("servicePort", "service", "port", 8081),
              ("serviceSoapPath", "service", "path", "SOAP"),
              ("serviceXmlPath", "service", "xmlPath", ""),
              ("servicePubPath", "service", "filePath", "pub"),
              ("serviceMode", "service", "mode", "master"),
              ("serviceLevel", "service", "level", "production"),
              ("servicePid", "service", "pidFile", "swamp.pid"),
              
              ("masterUrl", "service", "masterUrl", ""),
              ("masterAuth", "service", "masterAuth", ""),
              
              ("dbFilename", "db", "filename", None),

              ]
    REMAP = {"True" : True, "False" : False, "Yes" : True, "No" : False,
             "" : None,
             "CRITICAL" : logging.CRITICAL,
             "ERROR" : logging.ERROR,
             "WARNING" : logging.WARNING,
             "INFO" : logging.INFO,
             "DEBUG" : logging.DEBUG,
             "NOTSET" : logging.NOTSET}

    staticInstance = None

    def __init__(self, fname = "swamp.conf"):
        self.config = ConfigParser.ConfigParser()
        self.slave = []
        if isinstance(fname, list):
            self.filepath = map(self._fixPath, fname)
        else:
            self.filepath = [self._fixPath(fname)]


    def _fixPath(self, fname):
        if len(os.path.split(fname)[0]) == 0:
            # Used to look in same place as the script is located...
            # but it seems more logical to check the current working
            # directory.
            filepath = os.path.join(os.getcwd(), fname)
        else:
            filepath = fname
        return filepath

    
    def read(self):
        log.info("Reading configfile %s" % (self.filepath))
        map(self.config.read, self.filepath) # read all configs in-order.
        for m in Config.CFGMAP:
            val = m[3] # preload with default
            if self.config.has_option(m[1], m[2]):
                val = self.config.get(m[1],m[2])
                if val in Config.REMAP:
                    val = Config.REMAP[val]
                elif not isinstance(val, type(m[3])):
                    val = type(m[3])(val.split()[0]) # coerce type to match default
            setattr(self, m[0], val)
            log.debug( "set config %s to %s"%(m[0], str(val)))
            pass
        self.postReadFixup()
        pass

    def postReadFixup(self):
        if self.execSlaveNodes > 0:
            urlStr = "slave%dUrl"
            slotStr = "slave%dSlots"
            self.slave = []
            for i in range(1, self.execSlaveNodes+1):
                u = self.config.get("exec", urlStr % i)
                s = int(self.config.get("exec", slotStr % i))
                self.slave.append((u, s))
                log.debug("Added slave: url=%s slots=%d" %(u,s))
            pass
        pass

    def mySoapUrl(self):
        return "http://%s:%d/%s/" % (self.serviceHostname,
                                     self.servicePort,
                                     self.serviceSoapPath)

    
    def dumpSettings(self, logger, level):
        template = "config %s = %s"
        for m in Config.CFGMAP:
            val = getattr(self, m[0])
            logger.log(level, template % (m[0], str(val)))

    def updateViaFile(self, file):
        self.newconfig = None
        pass
    def update(self, overrides):
        if overrides:
            log.warning("Unimplemented configuration overriding code.")
        pass

    def writePid(self, filename=None):
        if not filename:
            filename = self.servicePidFile
        try:
            open(filename, "w").write(str(os.getpid())+"\n")
        except:
            log.warning("Couldn't write pid to %s" % filename)
        pass
            
    @staticmethod
    def dummyConfig():
        """Make a placeholder dummy config.  Our dummy config is one
        that is loaded with the default values."""
        c = Config()
        for m in Config.CFGMAP:
            val = m[3] 
            setattr(c, m[0], val)
        return c
        

def testConfigReader():
    logging.basicConfig(level=logging.DEBUG)
    c = Config()
    c.read()

def main():
    testConfigReader()

if __name__ == '__main__':
    main()
