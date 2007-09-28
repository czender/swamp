#!/usr/bin/python
# $Id$

"""
workerServer - A frontend 'driver' to start a server instance

"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import getopt
import sys
import swamp.server


def test():
    swamp.server.pingServer("slave.conf")
    
def main():
    confname = "slave.conf"
    # Split this config handling to something scalable
    # if once we have >2 options
    (opts, leftover) = getopt.getopt(sys.argv[1:], "c:",["config="])
    offeredConfs = []
    for o in opts:
        if (o[0] == "-c") or (o[0] == "--config"):
            offeredConfs.append(o[1])
    if offeredConfs:
        confname = offeredConfs

    swamp.server.startServer(confname)

if __name__ == '__main__':
    # degenerate test-mode
    if (len(sys.argv) > 1) and sys.argv[1] == "-t":
        test()
    else:
        main()

