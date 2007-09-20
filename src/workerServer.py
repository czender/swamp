#!/usr/bin/python
# $Id$

"""
workerServer - A frontend 'driver' to start a server instance



"""
# Copyright (c) 2007 Daniel Wang
# This file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

import swamp.server
import sys

def test():
    swamp.server.pingServer("slave.conf")
    
def main():
    swamp.server.startServer("slave.conf")

if __name__ == '__main__':
    if (len(sys.argv) > 1) and sys.argv[1] == "-t":
        test()
    else:
        main()

