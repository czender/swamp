# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
subproc - contains code for doing subprocess calls.
This wraps up the workaround we need to do because Twisted
interferes with Python's popen/subprocess/spawn calling.

"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import subprocess
import threading
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet.utils import getProcessOutputAndValue

from swamp import log


usingTwisted = False # set this to True to use Twisted subprocess management.


    
def call(executable, args):
    if usingTwisted:
        return uglyCall(executable, args)
        return twistedCall(executable, args)
    return pythonCall(executable, args)

def pythonCall(executable, args):
    proc = subprocess.Popen(executable=executable,
                            args=args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    output = proc.communicate()[0] #Save output (future use)
    code = proc.returncode
    return [code, output]

def uglyCall(executable, args):
    # use errno-513-resistant method of execution.
    code = None
    while code is None:
        try:
            code = os.spawnv(os.P_WAIT, executable, args)
        except OSError, e:
            if not (e.errno == 513):
                raise
            pass #retry on ERESTARTNOINTR
    return [code, ""]

# Re-define two functions that Twisted uses.
from twisted.python import log as tLog
from twisted.internet.process import unregisterReapProcessHandler
import os
def reapProcess(self):
    """Try to reap a process (without blocking) via waitpid.
    
    This is called when sigchild is caught or a Process object loses its
    "connection" (stdout is closed) This ought to result in reaping all
    zombie processes, since it will be called twice as often as it needs
    to be.

    (Unfortunately, this is a slightly experimental approach, since
    UNIX has no way to be really sure that your process is going to
    go away w/o blocking.  I don't want to block.)

    We replace Twisted's version with another that tolerates a
    non-integer self.pid.
    """
    try:
        pid, status = os.waitpid(self.pid, os.WNOHANG)
    except:
        tLog.msg('Failed to reap %s:' % str(self.pid)) # This line changed.
        tLog.err()
        pid = None
    if pid:
        self.processEnded(status)
        unregisterReapProcessHandler(pid, self)

from twisted.internet.process import reapProcessHandlers
def registerReapProcessHandler(pid, process):
    if reapProcessHandlers.has_key(pid):
        raise RuntimeError
    try:
        aux_pid, status = os.waitpid(pid, os.WNOHANG)
    except:
        log.msg('Failed to reap %s:' % str(pid)) # This line changed.
        log.err()
        aux_pid = None
    if aux_pid:
        process.processEnded(status)
    else:
        reapProcessHandlers[pid] = process


# Replace Twisted's code with one that doesn't crash (Python 2.4.2, amd64)
import twisted.internet.process
twisted.internet.process.Process.reapProcess = reapProcess
twisted.internet.process.Process.registerReapProcessHandler = registerReapProcessHandler

def mkCallback(code, result, sema):
    def callback(tup):
        out,err,code = tup
        result.append(0)
        result.append(out+err)
        sema.release()
    return callback

        

def twistedCall(executable, args):
    """Work-around Twisted's insistence on using callbacks for its subprocess
    code, which we must use because Twisted breaks Python's builtin
    subprocess module.  This is disgusting enough that we should consider
    doing-away with our thread-pooling model."""
    result = []
    sema = threading.Semaphore(0)
    #print executable,"---",args
    d = getProcessOutputAndValue(executable=executable, args=args[1:])
    d.addCallback(mkCallback(0,result,sema))
    d.addErrback(mkCallback(-1,result,sema))
    sema.acquire() # Block until it's ready.
    #print "returning result",result
    return result

