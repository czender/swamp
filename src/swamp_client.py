#!/usr/bin/python
# $Id$
# $URL$
#
# swamp_client.py - A client interface to use with a swamp server.
#
# This file is part of the SWAMP project and is released under
# the terms of the GNU General Public License version 3 (GPLv3)
# Copyright 2007 - Daniel L. Wang 


# Standard Python imports
import getopt
import os
import readline
import socket
import sys
import time
import urllib

# (semi-) third-party module imports
import SOAPpy

# SWAMP imports
from swamp_transact import *


class local:
    (localPath, scriptName) = os.path.split(sys.argv[0])
    helpStr = """    A_GREAT_NAME provides a handy command-line
    interface to a SWAMP SOAP server.
    
    Usage: A_GREAT_NAME [options] <analysis.sh> 

    -- <analysis.sh> is the name of a server-side script.  Syntax is
       POSIX-like*: a growing subset of standard POSIX is allowed.  You
       may invoke standard NCO binaries.

    Options:

    -h     Print this help.
    --help

    -u     Specify a different URL for the SWAMP SOAP interface 
    --url  (default: value in environment variable SWAMPURL)

    -c <configfile>
    --config <configfile>
           You may specify a configuration file that contains
           the desired settings.

    -t     Invoke a predefined, hardcoded Test that probably only works
    --test on the developer's test server.

    --backdoor
           Start a backdoor interface to the server that's really useful
           for development debugging, but should definitely be disabled
           for any servers open to the outside world. (Debug only)

    --reset
           Reset the state of the server.  This should also probably be
           disabled. (Debug only)

    Operation:
        A_GREAT_NAME submits the analysis script to be run on the server
        specified in the environment variable SWAMPURL (or overridden on the
        command line).  It then blocks until the analysis terminates, at
        which point it downloads the outputs and places them in the
        current directory.

    Returns: 0 on success.

    """
    # consider generating the option list from the option structure
    # used in the swampclient class.
    helpStr = helpStr.replace("A_GREAT_NAME", scriptName)

class SwampClient:
    def __init__(self):
        self._configFromEnv()
        self.operation = []
        self.didHelpPrintout = False
        pass

    def _configFromEnv(self):
        self.serverUrl = os.getenv("SWAMPURL")
        pass
    
    def _configFromFile(self, cfgfile="swampsoap.conf"):
        from swamp_config import Config
        conf = Config(cfgfile)
        self.serverUrl = "http://localhost:%d/%s" %(serverConf.serverPort,
                                                    serverConf.serverPath)
        pass

    def _sanityTest(self):
        if not self.serverUrl:
            print "empty serverUrl"
            return False
        return True
    
    def _testInterface(self):
        server = SOAPpy.SOAPProxy(self.serverUrl)
        print "Entering python debug console: (Ctrl-D/EOF exits)"
        try:
            while True:
                print server.pyInterface(raw_input())
        except EOFError: # catch the EOF error as normal exit
            pass
    
    def _test(self):
        if not self.serverUrl:
            # apply hardcoded url
            self.serverUrl = 'http://localhost:8081/SOAP'
        server = SOAPpy.SOAPProxy(self.serverUrl)
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
            continue
        
        outUrls = server.pollOutputs(tok)
        print "Debug: pollOutputs returned ", outUrls
        for u in outUrls:
            # simple fetch, since we are single-threaded.
            urllib.urlretrieve(u[1], u[0])
        
        pass


    def _addOperation(self, func):        
        self.operation.append(func)
        # perhaps consider more sophisticated queuing?
        pass

    def _handleConfigOption(self, configfile):
        self.configFile = configfile
        self._configFromFile(configfile)
        pass

    def _handleHelpOption(self, dummy=None):
        # for now, don't queue, just print it out and continue
        # may wish to obliterate other options and force a
        # print-help-only mode if any help is requested.
        print local.helpStr
        pass

    def _handleUrlOption(self, url):
        self.serverUrl = url
        pass

    def _handleTestOption(self, dummy=None):
        def testfunction():
            self._sanityTest()
            self._test()
        self._addOperation(testfunction)
        pass

    def _handleBackdoorOption(self, dummy=None):
        self._addOperation(self._testInterface)
        pass

    def _handleResetOption(self, dummy=None):
        self._addOperation(self._sendReset)
        pass

    def _sendReset(self):
        if not self._sanityTest():
            print "Failed sanity check.  Bailing out."
            return
        server = SOAPpy.SOAPProxy(self.serverUrl)
        result = server.reset()
        print "Server.reset returned", result
        pass

    def _processScript(self, scriptfile):
        print "Processing", scriptfile
        if not self._sanityTest():
            print "Failed sanity check.  Bailing out."
            return
        server = SOAPpy.SOAPProxy(self.serverUrl)
        # read file.
        if not os.access(scriptfile, os.F_OK | os.R_OK):
            print "can't read specified script file('%s')" % scriptfile
            return
        script = open(scriptfile).read()
        print "Attempting connection"
        try:
            tok = server.newScriptedFlow(script)
        except socket.error, e:
            print "Error connecting to server.  Check URL:", self.serverUrl
            print "Other possible reasons: firewalling, server downtime"
        print "Submitted", scriptfile

        ret = None
        self._waitForScriptFinish(tok)
        pass

    def _stringifyExtra(self, extra):
        if isinstance(extra, dict):
            if "commandCount" in extra and "executedCount" in extra:
                return "Executed %d/%d" %(extra["executedCount"],
                                          extra["commandCount"])
        return str(extra)
    
    def _waitForScriptFinish(self, token):
        server = SOAPpy.SOAPProxy(self.serverUrl)
        lastreport = None
        lastblip = 0
        lastextra = None
        while True:
            ret = server.pollState(token)
            state = SwampTaskState.newFromPacked(ret)
            if state.stable():
                print "Task finished, code ", state.name()
                break
            else:
                if state.name() != lastreport:
                    lastreport = state.name()
                    print "Task is in the (%d) %s state" %(state.state,
                                                           lastreport) 
                    if state.extra:
                        lastextra = self._stringifyExtra(state.extra) 
                        print "(", lastextra, ")"
                else:
                    lastblip += 1
                    if state.extra:
                        newextra = self._stringifyExtra(state.extra)
                        if newextra != lastextra:
                            lastextra = newextra
                            print "(", newextra, ")"
                    elif not(lastblip % 2):
                        print ".",
                    
            time.sleep(1)
            continue
        if state.name() != "finished":
            print "Execution error: ", state
            if state.extra:
                print "--",state.extra
            return
        outUrls = server.pollOutputs(token)
        for u in outUrls:
            # simple fetch, since we are single-threaded.
            print "Fetching %s and writing to %s" % (u[1], u[0])
            urllib.urlretrieve(u[1], os.path.expanduser(u[0]))
        # cleanup afterwards, discard published files.
        server.discardFlow(token) 
        pass

    def arguments(self, argv):
        """Applies command-line arguments to configure the client for
        its run.  Expects raw sys.argv or similar (argv[0] = script name)

        precondition:  ?
        postcondition: set a bunch of variables:
          operation -- "run function" to call (or a list of them)
          serverUrl -- valid url to connect to the server
          
        
        """
        # maintain the list of getopt arguments
        opttuples = [("c:", "config=", self._handleConfigOption),
                     ("h", "help", self._handleHelpOption),
                     ("u:", "url=", self._handleUrlOption),
                     ("t", "test", self._handleTestOption),
                     ("", "backdoor", self._handleBackdoorOption),
                     ("", "reset", self._handleResetOption)]

        # build getopt structures
        sopts = "".join(map(lambda t: t[0], opttuples))
        lopts = map(lambda t: t[1], opttuples)
        self.operation = [] # clear out operation queue
        (args, leftover) = getopt.getopt(argv[1:], sopts, lopts)

        # make a dict to handle options, mapping argument to operation
        # filter out bad tuples (the null version when an option does not
        # have both short and long forms)
        odict = dict(filter(lambda t: t[0],
                            map(lambda t: ('-' + t[0].rstrip(':'),
                                           t[2]), opttuples)
                            + map(lambda t: ('--' + t[1].rstrip('='),
                                             t[2]), opttuples)))
        map(lambda a: odict[a[0]](a[1]), args)  # handle all detected options
        
        # now, generate operations for each script file specified
        map(lambda f: self._addOperation(lambda : self._processScript(f)),
            leftover)

        # for otherwise "action-less" invocations, print the help
        # if it hasn't been printed.
        if (not self.operation) and (not self.didHelpPrintout):
            self._addOperation(lambda : self._handleHelpOption())
        
        pass

    def run(self):
        """run() performs whatever actions were dictated by its
        current settings.  """
        while self.operation:
            self.operation.pop(0)() # pop and invoke a queued operation
        

def main():
    c = SwampClient()
    c.arguments(sys.argv)
    c.run()



if __name__ == '__main__':
    main()
