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

SwampSoapInterfaceVersion = "$Id$"
log = logging.getLogger("SWAMP")

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
            self.config = Config(cfgName)
        else:
            self.config = Config()
        self.config.read()
        le = LocalExecutor.newInstance(self.config)
        self.filemap = FileMapper("f"+str(os.getpid()),
                                  self.config.execSourcePath,
                                  self.config.execResultPath,
                                  self.config.execResultPath)
        self.exportPrefix = "http://%s:%d/%s" % (self.config.serverHostname,
                                                 self.config.serverPort,
                                                 self.config.serverFilePath)
        self.resultExportPref = self.exportPrefix + "/"

        self.swampInterface = SwampInterface(self.config, le)
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
        
    def discardFlow(self, token):
        task = self.jobs[token]
        task.outMap.cleanPhysicals()
        self.discardedJobs[token] = self.jobs.pop(token)
        log.debug("discarding for token %d" %(token))
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

    def pollState(self, token):
        if token not in self.jobs:
            time.sleep(0.2) # possible race
            if token not in self.jobs:
                log.warning("token not ready after waiting.")
                return SwampTaskState.newState(token, "missing").packed()
        if isinstance(self.jobs[token], threading.Thread):
            return SwampTaskState.newState(token, "submitted").packed()

        #log.debug("trying exec poll" + str(self.jobs) + str(token))
        # for now, if the interface is there,
        #things are complete/okay.
        if isinstance(self.jobs[token], SwampTask):
            task = self.jobs[token]
            r = task.result()
            if r == True:
                return SwampTaskState.newState(token, "finished").packed()
            elif r != None:
                return SwampTaskState.newState(token, "generic error",r).packed()
            else:
                # is the task running?
                pos = self.swampInterface.queuePosition(task)
                if pos >= 0:
                    if pos == 0:
                        msg = "Queued: Next in line"
                    elif pos > 0:
                        msg = "Queued: %d ahead in line" % pos
                    return SwampTaskState.newState(token,
                                                   "waiting",
                                                   msg).packed()
                extra = task.status()
                return SwampTaskState.newState(token, "running",extra).packed()

                                                   
                          
        log.error("SOAP interface found weird object in self.jobs:" +
                  "token(%d) has %s" %(token, str(self.jobs[token])) )
        return SwampTaskState.newState(token, "system error").packed()

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
            log.info("Got request for %s which is not available")
            return self.resultExportPref
        else:
            return self.resultExportPref + relative[1]
    
    def pollOutputs(self, token):
        assert token in self.jobs
        task = self.jobs[token]
        outs = task.scrAndLogOuts
        log.debug(str(outs))

        outUrls = map(lambda f: (f[0], self.actualToPub( # make url from file
            task.outMap.mapReadFile(f[1]))), # find output localfile
                       outs) #start from logical outs.
        log.debug(str(outUrls))

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

    def startTwistedServer(self):
        from twisted.internet import reactor
        root = tResource.Resource()
        pubRes = tStatic.File(self.config.execResultPath)
        tStatic.loadMimeTypes() # load from /etc/mime.types
        root.putChild(self.config.serverFilePath, pubRes)
        root.putChild(self.config.serverPath, TwistedSoapSwampInterface(self))
        #root.putChild("fx", Hello(self.config))
        self.config.serverInspectPath = "inspect"
        self.config.runtimeJobManager = self
        root.putChild("inspect", InspectorResource(InspectorInterface(self.config)))

        reactor.listenTCP(self.config.serverPort, tServer.Site(root))
        log.debug("starting swamp SOAP ")
        reactor.run()
        pass

    def grimReap(self):
        self.swampInterface.grimReap()
        
    pass # end class StandardJobManager

class ScriptContext:
    """Contains objects necessary to manage *ONE* script's running context.
    ***This isn't really needed anymore.  Verify that we don't need
    any logic or ideas from here, and then delete this class.
    """
    
    def __init__(self, config):
        self.config = config

        self.sched = Scheduler(config, None) # build schedule without executor.
        self.commandFactory = CommandFactory(config)
        self.parser = Parser()
        self.parser.updateVariables({
            "SWAMPVERSION" : "0.1+",
            "SHELL" : "swamp",
            "SWAMPHOSTNAME" : self.config.serverHostname,
            })
        
        self.taskId = self.sched.makeTaskId()
        pass
    def addScript(self, script):
        self.script = script
        
        pass

    def addTree(self, tree):
        raise StandardError("Tree accepting not implemented")
    

    def id(self):
        return self.taskId
    
    def run(self, context):
        # actually, want to request resources from the system,
        # then build control structures, and then execute.
        self.sched.executeParallelAll(self.remote)
        pass
    pass

class InspectorResource(tResource.Resource):
    def __init__(self, interface):
        tResource.Resource.__init__(self)
        self.interface = interface
        
    def render_GET(self, request):
        if "action" in request.args:
            action = request.args["action"][0]
            flattenedargs = dict(map(lambda t:(t[0],t[1][0]), request.args.items()))
            return self.interface.execute(action, flattenedargs, lambda x:None)
            
    
    def getChild(self, name, request):
        if name == '':
            return self
        return tResource.Resource.getChild(
            self, name, request)


class InspectorInterface:
    def __init__(self, config):
        self.actions = {#"rebuilddb": self.rebuildDb,
                        #"showdb": self.showDb,
                        "catalog" : self.catalog,
                        "help": self.printHelp,
                        #"ls" : self.listFiles,
                        "joblist" : self.listJobs,
                        "env" : self.showEnv,
                        "filedb" : self.showFileDb,
                        "sanitycheck" : self.sanityCheck
                        }
        self.config = config
        self.endl = "<br/>"
        
    def buildUrl(self, action):
        return  "http://%s:%d/%s?action=%s" % (self.config.serverHostname,
                                        self.config.serverPort,
                                        self.config.serverInspectPath,
                                        action)

    def handyHeader(self):
        pre = '<span id="toolbar"> Handy Toolbar: '
        bulk = ' | '.join(map( lambda x : '<a href="%s">%s</a>' 
                               % (self.buildUrl(x),x), self.actions.keys()))
        post = '</span>'
        return "".join([pre,bulk,post])
    def rebuildDb(self,form):
        """resets the db, clearing entries and using the latest schema"""
        import swamp_dbutil
        print self.handyHeader()
        print "dbfilename is ", dbfilename
        try:
            swamp_dbutil.deleteTables(dbfilename)
        except:
            pass # ok if error deleting tables.
        swamp_dbutil.buildTables(dbfilename)
        
        return "Done rebuilding db"
    def showDb(self,form):
        """prints the db state"""
        import swamp_dbutil
        print self.handyHeader()
        swamp_dbutil.quickShow(dbfilename)
        
        return "done with output"
    def showFileDb(self, form):
        """prints the filestate in the db"""
        import swamp_dbutil
        print self.handyHeader()
        swamp_dbutil.fileShow(dbfilename)
        return "done with output"
    def printHelp(self,form):
        """prints a brief help message showing available commands"""
        r = self.handyHeader()
        r += "\n<pre>available commands:\n"
        for a in self.actions:
            r += "%-20s : %s\n" %(a, self.actions[a].func_doc)
        r += "</pre>\n"
        return r
        
        return "done with output"
    def showEnv(self, form):
        """(debug)prints the available env vars"""
        result = [ self.handyHeader()]
        result.append("<pre>")
        for k in os.environ:
            result.append( "%-20s : %s" %(k,os.environ[k]))
        result.append("</pre>")
        return self.endl.join(result)

    def listJobs(self, form):
        """Get a list of the jobs/workflows tracked by the system"""
        donejobs = self.config.runtimeJobManager.discardedJobs
        def info(task):
            if task:
                return "Task with %d logical outs, submitted %s" % (
                    len(task.logOuts), time.ctime(task.buildTime))
            else:
                return ""
        def fixlist(items):
            elems = map(lambda x:"%d -> %s" % (x[0], info(x[1])), items)
            return self.endl.join(elems)

        donejobs = fixlist(donejobs.items())
        runjobs = fixlist(self.config.runtimeJobManager.jobs.items())
        
        i = self.config.runtimeJobManager.swampInterface
        officialreport = i.execSummary()
        report = self.endl.join([info(officialreport[0]),
                                 " ".join(map(info,officialreport[1])),
                                 " ".join(map(info,officialreport[2]))])
        return "".join([ self.handyHeader(), self.endl,
                         "submitted jobs:", self.endl,
                         runjobs, self.endl,
                         "discarded:", self.endl,
                         donejobs])

    def _osFind(self, *paths ):
        """Roughly, an implementation of standard unix 'find'.
        Implementation borrowed from:
        http://www.python.org/search/hypermail/python-1994q2/0116.html
        by Steven D. Majewski (sdm7g@elvis.med.Virginia.EDU)"""
        list = []
        expand = lambda name: os.path.expandvars(os.path.expanduser(name))
        def append( list, dirname, filelist ):
            DO_NOT_INCLUDE=set([".",".."])
            filelist.sort()
            for filename in filelist:
                if filename not in DO_NOT_INCLUDE:
                    filename = os.path.join( dirname, filename )
                    if not os.path.islink( filename ):
                        list.append( filename )

        for pathname in paths:
            os.path.walk( expand(pathname), append, list )
        return list
    
    def sanityCheck(self, form):
        """Check some internal data structures for consistency"""
        versions = ["Swamp core version: %s" % SwampCoreVersion,
                    "Swamp SOAP interface version: %s" % SwampSoapInterfaceVersion]
        return self.endl.join([self.handyHeader()] + versions +
                              [ "No checks implemented yet"])

    def _rawCatalog(self, root=""):
        prefix = self.config.execSourcePath
        topchildren = os.listdir(prefix)
        files = self._osFind(prefix)
        ncfiles = filter(lambda s: s.endswith(".nc") and s.startswith(prefix),
                         files)
        sanitized = map(lambda s: (s[len(prefix):], os.stat(s).st_size),
                        ncfiles)
        return sanitized

    def catalog(self, form):
        """Get a listing of the files available for SWAMP to read"""
        commaize = lambda n: (str(n),
                              (n>999) and commaize(n/1000)+ ",%03d" % (n%1000) )[n>999]
        rawcat = self._rawCatalog()
        makeitemline = lambda t: (os.path.split(t[0])[0],
                                  "%s -- %s" %(t[0],commaize(t[1])))
        itemlines = map(makeitemline, rawcat)

        orgitemlines = []
        xlt = string.maketrans("/%","__")
        safename = lambda x: x.translate(xlt)
        targetline = lambda x: "<a name=\"%s\"><font size=+1>%s</font></a>" % (safename(x), x)
        dirlist = []
        def insertptr(oldline, lines, dlist):
            p = oldline[0]
            if (not dlist) or (p != dlist[-1]):
                lines.append(targetline(p))
                dirlist.append(p)
            lines.append(oldline[1])

        map(lambda x: insertptr(x,orgitemlines,dirlist), itemlines)
        dirstring = " ".join(map(lambda x: "<a href=\"#%s\">%s</a><br/>" %
                                 (safename(x),x), dirlist))
        return self.endl.join([self.handyHeader(), dirstring] + orgitemlines )

        
    
    def listFiles(self,form):
        """does a normal ls file listing. sorta-secure"""
        print self.handyHeader()
        if not form.has_key("path"):
            print "no path specified, specify with parameter 'path'"
            return
        path = form["path"].strip()
        if path.startswith("/") or path.startswith("../") \
           or (path.find("..") > -1):
            print "invalid path specified, try again"
            return
        ppath = os.path.join(os.getenv("DOCUMENT_ROOT"),path)
        print "<pre>BEGIN listing for :",path
        #no leading /
        try:
            for a in os.listdir(ppath):
                print a
        except OSError:
            print "END Error using path ", path
        print "END listing</pre>"
        
        return "done with output"
        
    def complainLoudly(self):
        """internal: print a nice error message if an unknown action
        is requested"""
        return self.endl.join([self.handyHeader(),
                               "Sorry, I didn't understand your request."])

    def execute(self, action, form, errorfunc):
        if action in self.actions:
            return self.actions[action](form)
        else:
            return self.complainLoudly()

class TwistedSoapSwampInterface(tSoap.SOAPPublisher):
    def __init__(self, jobManager):
        self.jobManager = jobManager
    def soap_reset(self):
        return self.jobManager.reset()
    def soap_newScriptedFlow(self, script):
        return self.jobManager.newScriptedFlow(script)
    def soap_discardFlow(self, token):
        return self.jobManager.discardFlow(token)
    def soap_pollState(self, token):
        return self.jobManager.pollState(token)
    def soap_pollOutputs(self, token):
        return self.jobManager.pollOutputs(token)
    def soap_pollJob(self, jobToken):
        return self.jobManager.pollJob(jobToken)
    def soap_pyInterface(self, cmdline): # huge security hole for debugging
        return self.jobManager.pyInterface(cmdline)

class Hello(tResource.Resource):
    def getChild(self, name, request):
        if name == '':
            return self
        return tResource.Resource.getChild(
            self, name, request)

    def render_GET(self, request):
        return """<html>
      Hello, world! I am located at %r.  Request contains: %s
    </html>""" % (request.prepath, dir(request) )
    def render_POST(self, request):
        magictoken = "56bnghty56" #make this site-configurable
        request.args["userfile"]
        return """<html>
      Hello, world! I am located at %r.  Request contains: %s .
      <BR/>
      I'm using the POST path.

      Your headers were %s

      Your args were %s

      Your content was %d bytes long
    </html>""" % (request.prepath, type(request), request.getAllHeaders(),
                  request.args, len(request.content.getvalue()))
        


class SwampExtInterface:
    
    def submitScript(self, script):
        """spawn a thread to get things started, assign a task id,
        and return it."""
        sc = ScriptContext(self.config)
        sc.addScript(script)
        taskid = sc.id()
        self.forkOff(sc)
        return taskid

    def submitTree(self, parsedFlow):
        """accept an already parsed, disambiguated, DAG workflow,
        and execute it"""
        sc = ScriptContext(self.config)
        sc.addTree(script)
        taskid = sc.id()
        self.forkOff(sc)
        return taskid

    
    def retrieveResults(self, taskid):
        """return a list of filenames and urls"""
        pass
    def discard(self, taskid):
        """free all resources associated with this taskid"""
        # this probably kills a job in progress.
        pass
        
        pass
    pass # end class SwampExtInterface
    

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

