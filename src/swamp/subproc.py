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


usingTwisted = False # set this to True to use Twisted subprocess management.


    
def call(executable, args):
    if usingTwisted:
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
