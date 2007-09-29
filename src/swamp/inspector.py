# $Id: soapi.py 32 2007-09-21 00:30:36Z daniel2196 $

"""
inspector - Contains logic for swamp's web-based inspector.

 Requires twisted.web http://twistedmatrix.com/trac/wiki/TwistedWeb

"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# Standard Python imports
import string
import os
import sys
import time

# (semi-) third-party imports
import twisted.web.resource as tResource

# SWAMP imports
import swamp



class Resource(tResource.Resource):
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


class Interface:
    def __init__(self, config):
        self.actions = {#"rebuilddb": self.rebuildDb,
                        #"showdb": self.showDb,
                        "catalog" : self.catalog,
                        "help": self.printHelp,
                        #"ls" : self.listFiles,
                        "joblist" : self.listJobs,
                        "env" : self.showEnv,
                        #"filedb" : self.showFileDb,
                        "sanitycheck" : self.sanityCheck,
                        "hardreset" : self.hardReset
                        }
        self.config = config
        self.endl = "<br/>"
        
        
    def _buildUrl(self, action):
        return  "http://%s:%d/%s?action=%s" % (self.config.serverHostname,
                                        self.config.serverPort,
                                        self.config.serverInspectPath,
                                        action)

    def _handyHeader(self):
        announcement = """<span id="info"><br/>
        Warning, this won't work properly on worker instances <br/></span>"""
        pre = '<span id="toolbar"> Handy Toolbar: '
        sorted = self.actions.keys()
        sorted.sort()
        bulk = ' | '.join(map( lambda x : '<a href="%s">%s</a>' 
                               % (self._buildUrl(x),x), sorted))
        post = '</span> ' + announcement
        return "".join([pre,bulk,post])

    def showFileDb(self, form):
        """prints the filestate in the db"""
        import swamp_dbutil
        swamp_dbutil.fileShow(dbfilename)
        return "done with output"

    def printHelp(self,form):
        """prints a brief help message showing available commands"""
        r = self._handyHeader()
        r += "\n<pre>available commands:\n"
        sorted = self.actions.items()
        sorted.sort()
        for (k,v) in sorted:
            r += "%-20s : %s\n" %(k, v.func_doc)
        r += "</pre>\n"
        return r
        
    def showEnv(self, form):
        """(debug)prints the available env vars"""
        result = [ self._handyHeader()]
        result.append("<pre>")
        if self.config.serverMode not in ["debug","testing"]:
            result.append("Only available in debug or testing service modes.")
        else:
            for k in os.environ:
                result.append( "%-20s : %s" %(k,os.environ[k]))
        result.append("</pre>")
        return self.endl.join(result)


    def listJobs(self, form):
        """Get a list of the jobs/workflows tracked by the system"""
        donejobs = self.config.runtimeJobManager.discardedJobs
        def info(task):
            if task:
                state = self.config.runtimeJobManager.taskStateObject(task)
                
                return "Task with %d logical outs, submitted %s : %s (%s)" % (
                    len(task.logOuts), time.ctime(task.buildTime),
                    state.name(), str(state.extra))
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
        return "".join([ self._handyHeader(), self.endl,
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
        versions = [# "Swamp core version: %s" % SwampCoreVersion,
                    "Swamp SOAP interface version: %s" % swamp.SoapInterfaceVersion]
        return self.endl.join([self._handyHeader()] + versions +
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
        return self.endl.join([self._handyHeader(), dirstring] + orgitemlines )

        
    
    def listFiles(self,form):
        """does a normal ls file listing. sorta-secure"""
        print self._handyHeader()
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

    def hardReset(self, form):
        """UNSAFELY restarts server process.(DANGEROUS)"""

        if self.config.serverMode != "debug":
            return "".join([self._handyHeader(),
                         "Error, debugging is disabled."])
        else:
            # Unless we thread this or use twisted deferred constructs
            # we can't provide a friendly response in this case.
            self._performReset() # will not return
        pass

    def _performReset(self):
        """
        Duplicates three lines of reset code in server.py.
        It would seem to require more than three lines to properly fix
        the plumbing so that we had *one* place for full-restart code.
        It shouldn't be that hard.  FIXME.
        """        
        args = sys.argv #take the original arguments
        args.insert(0, sys.executable) # add python
        os.execv(sys.executable, args) # replace self with new python.
        print "Reset failed:",sys.executable, str(args)
        return # This will not return.  os.execv should overwrite us.

    def _complainLoudly(self):
        """internal: print a nice error message if an unknown action
        is requested"""
        return self.endl.join([self._handyHeader(),
                               "Sorry, I didn't understand your request."])

    def execute(self, action, form, errorfunc):
        """Used to wrap up the function dispatch with mild error-handling."""
        if action in self.actions:
            return self.actions[action](form)
        else:
            return self._complainLoudly()

def newResource(config):
    return Resource(Interface(config))
