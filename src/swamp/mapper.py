# $Id: server.py 36 2007-09-26 01:18:06Z daniel2196 $

"""
mapper - contains code for bookkeeping file maps


"""
# Copyright (c) 2007 Daniel L. Wang, Charles S. Zender
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import os
from swamp import log

class FileMapper:
    def __init__(self, name, readParent, writeParent, bulkParent,
                 panicSize=3000000000):
        # we assume that logical aliases are eliminated at this point.
        self.physical = {} # map logical to physical
        self.logical = {} # map physical to logical
        self.readPrefix = readParent + os.sep 
        self.writePrefix = writeParent + os.sep + name + "_"
        self.writeParent = writeParent
        self.bulkPrefix = bulkParent + os.sep + name + "_"
        self.bulkParent = bulkParent
        self.panicSize = panicSize
        pass

    def clean():
        pass
    def existsForRead(self, f):
        return os.access(self.mapReadFile(f), os.R_OK)

    def mapReadFile(self, f):
        if f in self.physical:
            return self.physical[f]
        else:
            return self.readPrefix + f
        pass

    def spaceLeft(self, fname):
        (head,tail) = os.path.split(fname)
        s = os.statvfs(head)
        space = (s.f_bavail * s.f_bsize)
        #log.info("ok to write %s because %d free" %(fname,space))
        # Enough space left if (available > paniclimit)
        return (s.f_bavail * s.f_bsize) > self.panicSize


    def mapWriteFile(self, f, altPrefix=None):
        if altPrefix is not None:
            pf = altPrefix + f
        else:
            pf = self.writePrefix + f
        if not self.spaceLeft(pf):
            pf = self.bulkPrefix + f
            log.info("mapping %s to bulk at %s" %(f,pf))
           
        self.logical[pf] = f
        self.physical[f] = pf
        return pf

    def mapBulkFile(self, f):
        return self.mapWriteFile(f, self.bulkPrefix)

    def _cleanPhysical(self, p):
        try:
            if os.access(p, os.F_OK):
                os.unlink(p)
            f = self.logical.pop(p)
            self.physical.pop(f)
        except IOError:
            pass

    def discardLogical(self, f):
        p = self.physical.pop(f)
        if os.access(p, os.F_OK):
            os.unlink(p)
            log.debug("Unlink OK: %s (%s)" %(f,p))
        self.logical.pop(p)

    
    def cleanPhysicals(self):
        physicals = self.logical.keys()
        map(self._cleanPhysical, physicals)

class LinkedMap(FileMapper):
    """LinkedMap is a filemap that is linked to a parent map.
    This class exists for those cases where an additional context
    is needed within the parent map that can be disambiguated.
    """
    def __init__(self, parent, pref):
        self.parent = parent
        self.pref = pref
        self.private = set()
        pass
        
    def existsForRead(self, f):
        if f in self.private:
            return self.parent.existsForRead(self.pref + f)
        else:
            return self.parent.existsForRead(f)
        pass
    
    def mapReadFile(self, f):
        if f in self.private:
            return self.parent.mapReadFile(self.pref + f)
        else:
            return self.parent.mapReadFile(f)
        pass
    
    def spaceLeft(self, fname):
        return self.parent.spaceLeft(fname)
        
    def mapWriteFile(self, f, altPrefix=None):
        self.private.add(f)
        return self.parent.mapWriteFile(self.pref + f, altPrefix)

    def mapBulkFile(self, f):
        self.private.add(f)
        return self.parent.mapBulkFile(self.pref + f)

    def discardLogical(self, f):
        if f in self.private:
            log.debug( "linked remove"+str(f))
            self.private.remove(f)
            return self.parent.discardLogical(self.pref + f)
        else:
            log.debug("tried to discard unmapped file " + f)
        pass

    def cleanPhysicals(self):
        map(lambda f: self.parent.discardLogical(self.pref + f),
            self.private)
        #assume discard success.
        self.private.clear()
        pass

