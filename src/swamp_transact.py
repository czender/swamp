#!/usr/bin/python
# $Id$
# $URL$
#
# swamp_transact.py - (user/)client-server transaction module
#
# This file is part of the SWAMP project and is released under
# the terms of the GNU General Public License version 3 (GPLv3)
# Copyright 2007 - Daniel L. Wang 


"""swamp_transact:  Client-server common transaction module.
This provides various state information/data/metadata useful for
both client and server to prevent them from depending on each other.

Because messages to/from SWAMP clients and servers occur using SOAP
messages, and we design for the possibility of non-Python clients
(servers too, in our dreams), we need to translate to/from primitive
data types.  This module should do the annoying and dirty parts of
that.
"""
__author__ = "Daniel L. Wang <wangd@uci.edu>"
#__all__ = ["Parser"] # I should fill this in when I understand it better
SwampTransactVersion = "$Id$"

# Python imports
import cPickle as pickle
from itertools import *
import base64

# Third party imports
# (none)

# SWAMP imports
# (should have none)

class SwampTaskState:
    """A state object used to describe the state of a running
    job/task/workflow.  Use the factory functions."""
    statelisting = [
        # (code, name, isStable)
        (10, ("submitted", False)),
        (20, ("waiting", False)),
        (101, ("generic error", True)),
        (110, ("parse error", True)),
        (120, ("execution error", True)),
        (150, ("system error", True)),
        (190, ("missing", False)),
        (210, ("running", False)),
        (310, ("finished", True)),
        (410, ("discarded", True)),
        (1000, ("invalidstate", True))
        ]
    # lookup table for state information, by state code
    state = dict(statelisting)
    # inverted table, indexing on text.
    statecode = dict(izip(imap(lambda x:x[0], state.itervalues()), state.iterkeys()))
    
    def __init__(self, token, state, extra):
        """Construct.
        token = integer identifier of this task in the system
        state = integer state code from statelisting
        extra = additional information specific to the state"""
        self.state = state
        self.token = token
        self.extra = extra
        pass

    def __str__(self):
        tup = SwampTaskState.state[self.state]
            
        return "TaskState[%d] %d %s (%s)" % (self.token, self.state,
                                             tup[0], str(self.extra))
    
    def name(self):
        """return the name of the state"""
        return SwampTaskState.state[self.state][0]

    def packed(self):
        """return a packed, base64 representation which will hopefully
        resist mangling by the SOAPpy implemenation.  Python's pickle
        module creates binary strings which foul up SOAPpy.
        """
        if self.extra:
            extra = base64.b64encode(pickle.dumps(self.extra))
        else:
            extra = None
        return (self.token, self.state, extra)

    def stable(self):
        """@return True if the state is "stable", that is, a client
        should not expect the task's state to change without intervention."""
        return SwampTaskState.state[self.state][1]

    @staticmethod
    def newState(token, statename, extra=None):
        """Construct and return a new state object.  This is a 'friendly'
        interface which should be used by code outside this class."""
        return SwampTaskState(token,
                              SwampTaskState.statecode[statename], extra)

    @staticmethod
    def newFromPacked(packed):
        """Make a state object from a packed representation,
        which a 2-tuple (for now) of (token,statecode)"""
        extra = packed[2]
        if not extra: # trivial case: no extra data
            return SwampTaskState(packed[0], packed[1], None)

        assert isinstance(extra, str) # should have been packed
        # When the time comes, put something here to unpickle
        # the packed base64. Otherwise, we assume it's a string
        return SwampTaskState(packed[0], packed[1],
                              pickle.loads(base64.b64decode(extra)))
    @staticmethod
    def classSanity():
        """Test consistency in code and data logic.
        Sanity is a necessary but insufficient indicator of correctness
        
        Return True if things are okay.
        """
        waiting = SwampTaskState.newState(1,"waiting")
        waiting2 = SwampTaskState.newFromPacked(waiting.packed())
        running = SwampTaskState.newState(1,"running", {"hello" : "world",
                                                        "foo" : 1})
        running2 = SwampTaskState.newFromPacked(running.packed())

        print "No exceptions while running"
        print running,running2
        return str(waiting) == str(waiting2) and str(running) == str(running2)

        
    pass

if __name__ == "__main__":
    assert SwampTaskState.classSanity()
