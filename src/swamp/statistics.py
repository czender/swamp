# $Id: $
"""
   statistics -- a module for handling script and performance statistics

"""

# 
# Copyright (c) 2007 Daniel Wang, Charles S. Zender
# This sample file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)
#



# Standard Python imports
import md5
import operator
import time

# (semi-) third-party imports
#import twisted.web.resource as tResource

# SWAMP imports
#import swamp

class ScriptStatistic:
    """A class that contains the statistics relevant to the execution
    of a single script. 
    """
    def __init__(self, script, task):
        self.startTime = time.time()
        self.finishTime = None
        self.parseFinishTime = 0
        self.script = script
        self.task = task
        self.hexhash = md5.md5(script).hexdigest()
        pass

    def outputFiles(self, filesizelist):
        """record measurements on output files"""
        self.outputs = filesizelist
        self.outputSize = reduce(lambda x,y: x + y[1], filesizelist, 0)

    def inputFiles(self, filesizelist):
        """record measurements on input files"""
        self.inputs = filesizelist
        self.inputSize = reduce(lambda x,y: x + y[1], filesizelist, 0)

    def commandList(self, clist):
        def printC(cmd):
            print "cmd has inputs", cmd.inputs, "and outputs",cmd.actualOutputs
            pass
        self.cmdList = clist
        #map(printC, clist)
        w = self._findWidth(clist)
        outs = set()
        map(lambda c: outs.update(c.actualOutputs), clist)
        finalouts = set(map(lambda x: x[0], self.outputs))
        intermeds = filter(lambda x: x[0] not in finalouts, outs)
        #print "finalouts:",finalouts
        #print "intermeds:",intermeds
        self.intermedSize = -1
        try:
            self.intermedSize = reduce(lambda x,y: x + y[2], intermeds, 0)
        except:
            pass # don't worry about problems here.

    def dotFormat(self, clist):
        """return list of strings that, concatenated, yield a graph
        specification suitable for processing by the dot graph renderer.
        This lets us make pretty workflow graphs via graphviz."""
        # implement me!
        
        pass
    

    def markParseFinish(self):
        self.parseFinishTime = time.time()
        
    def stop(self):
        self.finishTime = time.time()        
        self.runTime = self.finishTime - self.startTime
        self.parseTime = self.parseFinishTime - self.startTime
        self.computeTime = self.finishTime - self.parseFinishTime
        pass
    
    def finish(self):
        """Mark as finished, and perform whatever else we need to do to
        close things down, e.g. calculate durations, flush to disk, etc.
        """
        if not self.finishTime:
            self.stop()
        print "flush script", self.runTime, "seconds"
        print "compute time", self.computeTime, "seconds"
        print "parse time", self.parseTime, "seconds"
        print "output size", self.outputSize
        print "input size", self.inputSize
        print "intermediate size", self.intermedSize
        print "overall tree width", self.dagWidth
        print "local slots", self.task.config.execLocalSlots

    def statList(self):
        return self._statListForClient()
    
    def lessThanEqual(self, rhs):
        return self.startTime < rhs.startTime

    def _findWidth(self, clist):
        """not working properly right now."""
        self._traversed = set()
        def traverse(cmd):
            if cmd in self._traversed:
                return 0
            return self._findNodeWidth(cmd)
        width = reduce(operator.add, map(traverse, clist), 0)
        self.dagWidth = width
        return width

    def _findNodeWidth(self, node):
        width = 1
        if node.children:
            relevantchildren = set(node.children).difference(self._traversed)
            width = reduce(operator.add,
                           map(self._findNodeWidth, relevantchildren),
                           0)
        self._traversed.add(node)
        #print "width of cmd line", node.referenceLineNum," is", width
        return width

    def _statListForClient(self):
        commaize = lambda n: (str(n),
                              (n>999) and commaize(n/1000)+ ",%03d" % (n%1000) )[n>999]

        return [("Execution Time", "%f seconds" % self.runTime),
                ("Input size",     "%s bytes" % commaize(self.inputSize)),
                ("Intermediate size", "%s bytes" % commaize(self.intermedSize)),
                ("Output size",    "%s bytes" % commaize(self.outputSize)),
                ("Estimated flow width", "%f" % self.dagWidth)
                ]

    def _dagGraph(self, cmdList):
        return ScriptStatistic.statDagGraph(cmdList)

    @staticmethod
    def statDagGraph(cmdList):
        otuples = []
        for c in cmdList:
            i = c.inputs
            o = c.outputs
            ituples = []
            for f in i:
                ituples.append([f,id(c)])
            if not i:
                ituples.append([id(c)])
            for f in o:
                for f2 in ituples:
                    otuples.append(f2+[f])
        return "\n".join(map(lambda t: " -> ".join(map(lambda s: '"%s"'%str(s),t)), otuples))
        pass
    def _partition(self, cmdList):
        b = Bipartitioner(cmdList)
        return b.result()
    def _writeScript(self):
        pass
        
class Bipartitioner:
    """Splits an approximately-min-cut partition of a flow graph."""
    def __init__(self, cmdList):
        self.original = cmdList
        
        total = len(cmdList)
        halftotal = total/2
        # arbitrarily split from ordered sequence
        self.sets = [set(cmdList[:halftotal]), set(cmdList[halftotal:])]
        
        locked = set() # locked nodes
        # while cutsize is reduced
        # while valid moves exist
        # use bucket data to find unlocked node in each partition that most improves cutsize
        (ba, bb) = self._makeBuckets(self.sets)
        print "Buckets One", ba
        print "Bucket Two", bb
        maxa = max(ba)
        maxa = (maxa, ba[maxa])
        maxb = max(bb)
        maxb = (maxb, bb[maxb])
        print "maxa",maxa,id(maxa[1][0])
        print "maxb",maxb,id(maxb[1][0])
        self._writeSetState("stage0")
        return
        if maxa[0] > maxb[0]:
            # move from a to b
            chain = maxa[1]
            e = chain.pop()
            setA.remove(e)
            setB.add(e)
            #find new gain:
            g = self._findGain(e, setB, setA)
            
            
        else:
            # move from b to a
            pass

        
        # make the move

        # lock moved node
        # update nets
        
        pass

    
    def _updateGain(self, c, bucket, current, remote):
        # remove from current chain
        gain = self._findGain(c, current, remote)
        # add to new chain.
        pass

        
    def _makeBuckets(self, sets):
        buck = [{},{}]
        # add a field to each command to eliminate need for extra table
        def fieldAdder(buckets, sets):
            def add(c):
                c.biPartBuckets = buckets
                c.biPartSets = sets
            return add
        map(fieldAdder(buck, sets), sets[0])
        (rb, rs) = (buck[:], sets[:])
        rb.reverse()
        rs.reverse()
        map(fieldAdder(rb,rs), sets[1])

        def addToBucket(bucket, cmd, a, b):
            gain = self._findGain(cmd, a,b)
            #print "looking for gain",gain, bucket[gain]
            bucket[gain] = bucket.get(gain,[]) + [cmd]
            #print "newgain",gain, bucket[gain]
        map(lambda c: addToBucket(buck[0], c, sets[0], sets[1]), sets[0])
        map(lambda c: addToBucket(buck[1], c, sets[1], sets[0]), sets[1])
        return buck


    def _findGain(self, cmd, source, dest):
        # gain(move) = number of cross-partition nets before - after
        # gain approximates the benefit from moving a command.
        # so, gain = -delta(cost) = -(cost_after - cost_before)
        # = cost_before - cost_after
        
        # before: num of neighbors in other part
        # after: num of neighbors in current part
        # assume cost=1, but can estimate later
        source = cmd.biPartBuckets[0]
        beforecount = len(filter(lambda x: x in dest,
                                 cmd.parents + cmd.children))
        aftercount = len(cmd.parents)+len(cmd.children) - beforecount
        return beforecount - aftercount

        
        pass

    def _writeSetState(self,label):
        stage0 = [
            "digraph stage0 {",
            "subgraph clustera { ",
            str(ScriptStatistic.statDagGraph(self.sets[0])),
            "}",
            "subgraph clusterb { ",
            str(ScriptStatistic.statDagGraph(self.sets[1])),
            "}",
            "}"]
        open("%s.dot" %label,"w").write("\n".join(stage0))
        pass
    
    def result(self):
        return None #FIXME
    

class Tracker:
    """A context for tracking statistics.  This is the top-level
    statistics class.  Will probably only want one of these per
    swamp-instance.
    """
    def __init__(self, config):
        self.script = {}
        
    def scriptStart(self, scriptTuple):
        """Log the start of a script.

        scriptTuple: a tuple of (key, script, task)
        key: a foreign key used to refer to the script in the future
        (relatively unique over the set of keys passed to the Tracker
        over its lifetime)
        script: a string containing the script contents.
        task: a SwampTask object.  Desired(?) to make it easier to derive
        statistics.
        
        """
        (key, script, task) = scriptTuple
        stat = ScriptStatistic(script, task)
        self.script[key] = stat
        return stat



    def scriptStat(self, key):
        return self.script[key]

    def _writeStat(self, scriptTuple):
        (key, script, task) = scriptTuple

        
        
    pass

_tracker = None

def initTracker(config):
    global _tracker
    _tracker = Tracker(config)
    
def tracker():
    assert _tracker is not None # consider opening up tracker.
    return _tracker
