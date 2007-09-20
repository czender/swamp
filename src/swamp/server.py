# $Id$

"""
server - Contains 'top-level' code for swamp server instances

"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import swamp.soapi as soapi

#class JobManager:
    
def selfTest():
    pass

def pingServer(configFilename):
    soapi.pingTest(configFilename)

def startServer(configFilename):
    selfTest()

    jm = soapi.JobManager(configFilename) 
    jm.listenTwisted()

    pass

