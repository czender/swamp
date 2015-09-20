# (unfinished, check back later) #
# Introduction #

SWAMP is a simple system, with simple requirements, but may not be obvious to install, especially since no installation wizard has been developed.  Here is a brief look at how to install.

## Quick Benefits and Consequences ##
Offering a SWAMP service has a important benefits to those publishing their data:
  * Drastically reduced bandwidth demand from the server.
  * Simple, familiar analysis through scripts of NCO.  No new language learning needed!
  * Efficient analysis execution. SWAMP parallelizes your script so you get your results faster... automatically.

However, there are a few issues that you may be aware of, as the server administrator:
  * Increased CPU load at the server.  Since SWAMP relocates analysis away from the user and towards the server, where the data resides, some amount of CPU load increase should be expected.
  * SWAMP does not authentication for use.  Until such a feature is added, you may wish to keep your SWAMP instance behind a firewall.  The worst danger is that a demanding script may take a long time to execute, and cause the request queue to be lengthy.  SWAMP is conservatively designed to never overload the server.  If your data is sensitive, a firewall is a must.
  * Disks hosting your data may be used more intensively.  Except for odd cases, this means your data is getting more usage, which is positive.  SWAMP enhances access to your data, and like other forms of access, can increase load.


# Before getting started #
Make sure your system meets the requirements:
  * Unix-like operating system with installed software:
    * [Python](http://python.org) 2.4 or higher
    * Twisted Web (0.5 or higher) [Twisted Matrix Labs](http://twistedmatrix.com/)
    * [SOAPpy](http://sourceforge.net/project/showfiles.php?group_id=26590&package_id=18246), from the [Python Web Services Project](http://pywebsvcs.sourceforge.net/), and its dependency, [fpconst](http://pypi.python.org/pypi/fpconst/0.7.2)
    * [pyparsing](http://pyparsing.wikispaces.com)
    * [netCDF Operators ](http://nco.sourceforge.net)

Most modern Linux distributions should have all of these in their repositories, and all are otherwise installable from installers or source packages<sup>1</sup>.  It _may_ be possible to make things work on a Windows platform.  Let us know if you'd like to try.

# Step 1: Download SWAMP service #
Although you can install using SVN, it's probably simpler to just get the swamp-service package and use it.  Link: [swamp-service-0.1rc4.tar.bz2](http://swamp.googlecode.com/files/swamp-service-0.1rc4.tar.bz2)

Once you've downloaded it, go ahead and unpack it into your directory of choice.
```
$ tar xjvf swamp-service-0.1rc4.tar.bz2
```

# Step 2: Configure SWAMP (edit swamp.conf) #
Copy the included sample-swamp.conf over to swamp.conf and edit it appropriately.  The file is annotated with brief descriptions of each option, but please ask if any help is needed.

# Step 3: Start SWAMP #
You may now start SWAMP with the commandline: `python swamp_soapinterface.py`  If your python installation is elsewhere, replace `python` with whatever is appropriate, e.g. `python24` on Rocks 4.2 .

# Step 4: Use SWAMP #
Your service is now running at the port you specified in the `swamp.conf` file.  A good test is to try the SWAMP client with it.  Using the sample scripts in the client package as examples, you can build a simple script to use on your own service.  File pathnames will be different, so you can use the built-in catalog to determine the paths that your installed service is using.  As an example, our own test server has its catalog viewable at: http://pbs.ess.uci.edu:8080/inspect?action=catalog

# Need help? #
Please direct questions to the SWAMP [user forum](http://groups.google.com/group/swamp-users):  [swamp-users@googlegroups.com](mailto:swamp-users@googlegroups.com).  We monitor this list and will respond as soon as we can.

## Notes ##
<sup>1</sup> Fedora, Debian, and Ubuntu have all of them in their package repositories (though Debian and older Ubuntu dists use old versions of NCO).  RHEL/CentOS coverage may be spotty-- please post to the forums if you would like to try our homebuilt RPMs for these platforms.