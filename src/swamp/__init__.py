
# Copyright (c) 2007 Daniel Wang
# SWAMP is released under the GNU General Public License version 3 (GPLv3)

"""
SWAMP: Script Workflow Analysis for MultiProcessing -- an
implementation of a distributed scientific computing service.
"""

# Ensure the user is running the version of python we require.
import sys
if not hasattr(sys, "version_info") or sys.version_info < (2,4):
    raise RuntimeError("swamp requires Python 2.4 or later.")
del sys

# setup version
__version__ = '0.1'

# setup all
__all__ = []

# Setup logger (? might move this to a separate file)
import logging
log = logging.getLogger("SWAMP")
