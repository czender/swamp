# Introduction #

SWAMP is designed to make it as easy as possible to go from zero to analysis.  In this document, we'll show you just how to do it.  If you're interested in installing and providing a SWAMP _service_, please see SwampServiceQuickstart.

# System Requirements #
  * Python 2.4 or higher
  * SOAPpy

# Preparation #
### Getting Python ###
Python is widely available for most platforms, and is included by default in many installations.  You may check your installed python version with the command `python -V` .  Some systems, such as CentOS4/RHEL4-based systems, install an earlier version of python by default, but allow version 2.4 to be installed in parallel.  In these cases, you may need to invoke python via `python2.4` instead of just `python`.  [Python main web site](http://python.org)

### Getting SOAPpy ###
SWAMP uses the SOAPpy package to provide Web Services functionality.  On many Linux systems, it is installable via the distributions' respective package managers, e.g. `aptitude install python-soappy` on Debian/Ubuntu or `yum install SOAPpy` on Fedora.  [SOAPpy web site](http://pywebsvcs.sourceforge.net)

# Installation #

### Getting SWAMP ###
Download the swamp client from http://swamp.googlecode.com .  The latest client is always a featured download.  `wget http://swamp.googlecode.com/files/swamp-client-0.1.tar.bz2`


### Unpack SWAMP ###
Unpack the client tarball.
`tar xvjf swamp-client-0.1.tar.bz2`

# Running #
Now, test SWAMP against our public test server.
```
cd swamp-client-0.1
export SWAMPURL='http://pbs.ess.uci.edu:8080/SOAP'
python swamp_client.py ipcctest.swamp 
```
### Notes ###
`http://pbs.ess.uci.edu:8080/SOAP` is the URL for our public test server.  The script file,  `ipcctest.swamp` , computes a time series of annual mean surface temperature for each of many climate models, and the ensemble mean of all models.  You can edit the script file to perform different functions to your liking.