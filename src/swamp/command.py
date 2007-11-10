# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
command - contains code that understands how to build commands
          (nodes in a workflow)


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

# Std. Python imports
import copy
import cPickle as pickle
import fnmatch
import glob
import operator
import os
import re


# (semi-)third-party imports
#from pyparsing import *


# SWAMP imports 
from swamp import log
#from swamp.syntax import BacktickParser


# Command and CommandFactory do not have dependencies on NCO things.
class Command:
    """this is needed because we want to build a dependency tree."""

    def __init__(self, cmd, argtriple, inouts, parents, referenceLineNum):
        self.cmd = cmd
        self.parsedOpts = argtriple[0] # adict
        self.argList = argtriple[1] # alist
        self.leftover = argtriple[2] # leftover
        self.inputs = inouts[0]
        self.outputs = inouts[1]
        self.parents = parents
        self.referenceLineNum = referenceLineNum
        self.actualOutputs = [] # [(logifile,physfile[,size]),...]
        self.children = []
        self.factory = None
        self.inputSrcs = []
        pass

    # sort on referencelinenum
    def __cmp__(self, other):
        return cmp(self.referenceLineNum, other.referenceLineNum)

    def __hash__(self): # have to define hash, since __cmp__ is defined
        return id(self)

    def pickleNoRef(self):
        safecopy = copy.copy(self)
        safecopy.parents = None
        safecopy.children = None
        safecopy.factory = None
        return pickle.dumps(safecopy)


    # need to map all non-input filenames.
    # apply algorithm to find temps and outputs
    # then mark them as remappable.
    # this can be done in the mapping function.
    def remapFiles(self, mapFunction):
        raise StandardError("unimplemented function Command.remapFiles")
    
    def remapFile(self, logical, mapInput, mapOutput):
        if logical in self.inputs:
            return mapInput(logical)
        elif logical in self.outputs:
            phy = mapOutput(logical)
            self.actualOutputs.append((logical, phy))
            return phy
        else:
            return logical

    def makeCommandLine(self, mapInput, mapOutput):
        cmdLine = [self.cmd]
        # the "map" version looks a little funny.
        #cmdLine += map(lambda (k,v): [k, self.remapFile(v,
        #                                            mapInput,
        #                                            mapOutput)],
        #               self.argList)
        cmdLine += reduce(lambda a,t: a+[t[0], self.remapFile(t[1],
                                                              mapInput,
                                                              mapOutput)],
                          self.argList, [])
            
        cmdLine += map(lambda f: self.remapFile(f, mapInput, mapOutput),
                       self.leftover)
        # don't forget to remove the '' entries
        # that got pulled in from the arglist
        return filter(lambda x: x != '', cmdLine)
        #self.cmdLine = filter(lambda x: x != '', cmdLine)
        #return self.cmdLine

    pass # end of Command class
        
class CommandFactory:
    """this is needed because we want to:
    a) connect commands together
    b) rename outputs.
    To create a new command, we need:
    a) which commands created my inputs?
    b) what should I remap the file to?
    So:
    -- a mapping: script filename -> producing command
    -- script filename -> logical name (probably the same,
    but may be munged)
    -- logical name -> producing command.  This is important
    for finding your parent.
    For each output, create a new scriptname->logical name mapping,
    incrementing the logical name if a mapping already exists.  
    
    """
    def __init__(self, config):
        self.config = config
        self.commandByLogicalOut = {}
        self.commandByLogicalIn = {}
        self.logicalOutByScript = {}
        self.scriptOuts = set() # list of script-defined outputs
        # scriptOuts is logicalOutByScript.keys(), except in ordering
        self.scrFileUseCount = {}
        self.scriptIns = set()
        pass

    def mapInput(self, scriptFilename):
        temps = fnmatch.filter(self.scriptOuts, scriptFilename)
        # FIXME: should really match against filemap, since
        # logicals may be renamed
        if temps:
            temps.sort() # sort script wildcard expansion.
            # we need to convert script filenames to logicals
            # to handle real dependencies
            return map(lambda s: self.logicalOutByScript[s], temps)
        else:
            inList = self.expandConcreteInput(scriptFilename)
            if inList:
                self.scriptIns.update(inList)
                return inList
            else:
                log.error("%s is not allowed as an input filename"
                              % (scriptFilename))
                raise StandardError("Input illegal or nonexistant %s"
                                    % (scriptFilename))
        pass

    def mapOutput(self, scriptFilename):
        s = scriptFilename
        if scriptFilename in self.logicalOutByScript:
            s = self.nextOutputName(self.logicalOutByScript[s])
        else:
            s = self.cleanOutputName(s)
        self.logicalOutByScript[scriptFilename] = s
        return s
                    
    def expandConcreteInput(self, inputFilename):
        # WARN: this may not be threadsafe! 
        save = os.getcwd()
        os.chdir(self.config.execSourcePath)
        # no absolute paths in input filename!
        s = inputFilename.lstrip("/")
        res = glob.glob(s)
        res.sort() # sort the wildcard expansion.

        os.chdir(save)
        return res

    def cleanOutputName(self, scriptFilename):
        # I can't think of a "good" or "best" way, so for now,
        # we'll just take the last part and garble it a little
        (head,tail) = os.path.split(scriptFilename)
        if tail == "":
            log.error("empty filename: %s"%(scriptFilename))
            raise StandardError
        if head != "":
            # take the last 4 hex digits of the head's hash value
            head = ("%x" % hash(head))[-4:]
        return head+tail

    def nextOutputName(self, logical):
        # does the logical name end with .1 or .2 or .3 or .99?
        # (has it already been incremented?)
        m = re.match("(.*\.)(\d+)$", logical)

        if m is not None:
            # increment the trailing digit(s)
            return m.group(1) + str(1 + int(m.group(2)))
        else:
            return logical + ".1"

    @staticmethod
    def incCount(aDict, key):
        aDict[key] = 1 + aDict.get(key,0)

    @staticmethod
    def appendList(aDict, key, val):
        l = aDict.get(key,[])
        l.append(val)
        aDict[key] = l

    def newCommand(self, cmd, argtriple,
                   inouts, referenceLineNum):
        # first, reassign inputs and outputs.
        scriptouts = inouts[1]
        newinputs = reduce(operator.add, map(self.mapInput, inouts[0]))
        newoutputs = map(self.mapOutput, inouts[1])
        inouts = (newinputs, newoutputs)

        # patch arguments: make new leftovers
        argtriple = (argtriple[0], argtriple[1], newinputs + newoutputs)

        # link commands: first find parents 
        inputsWithParents = filter(self.commandByLogicalOut.has_key, newinputs)
        map(lambda f: CommandFactory.incCount(self.scrFileUseCount,f),
            inputsWithParents)
        
        parents = map(lambda f: self.commandByLogicalOut[f], inputsWithParents)

        
        # build cmd, with links to parents
        c = Command(cmd, argtriple, inouts, parents, referenceLineNum)

        # then link parents back to cmd
        map(lambda p: p.children.append(c), parents)

        # update scriptOuts 
        self.scriptOuts.update(scriptouts)
        
        # update parent tracker.
        for out in inouts[1]:
            self.commandByLogicalOut[out] = c

        # link inputs to find new cmd
        map(lambda f: CommandFactory.appendList(self.commandByLogicalIn,f,c),
            inputsWithParents)

        # additional cmd properties to help delete tracking
        c.factory = self
        c.inputsWithParents = inputsWithParents
        return c

    def unpickleCommand(self, pickled):
        # not sure this needs to be done in the factory
        # -- maybe some fixup code though
        return pickle.loads(pickled)

    def realOuts(self):
        """return tuples of (scriptname, logname)
        for each file that is a final output """
        # technically, we should ask just for its keys,
        #but random access to a dict is faster.
        temps =  self.commandByLogicalIn

        if False:
            # original
            # get the final set of logical outs
            realouts = filter(lambda x: x not in temps,
                              self.commandByLogicalOut.keys())
            # then, take the final set and then find the script names
            scriptByLogical = dict([[v,k] for k,v
                                    in self.logicalOutByScript.items()])
            # ? 
        else: # new method:
            # Get the script outs, and then map them to logical outs.
            rawscriptouts = self.logicalOutByScript.items()
            scriptAndLogicalOuts = filter(lambda x: x[1] not in temps,
                              rawscriptouts)
            

        return scriptAndLogicalOuts

    pass # end of CommandFactory class
