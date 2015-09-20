## What is SWAMP? ##
SWAMP (the Script Workflow Analysis for MultiProcessing) is a system which aims to relocate scientists' existing data-analyzing shell-scripts to be executed safely and correctly on the data server.  SWAMP was conceived from geoscientists' growing discontent over the hassle of downloading and analyzing each others' data.  Climate and other earth-system models continue to produce high volumes of data, and the gap between data size and practically available bandwidth continue to grow, thus a system that integrates computation with data safely is crucial to understand and spur greater scientific discovery.

SWAMP integrates a frontend SOAP-based server with a cluster-aware scheduler that understands data sizes and standard POSIX shell syntax.

## So, really, what is it? (Scientist-version) ##
SWAMP is a software system that you (or your local computer person) can install on the data server for your group.  It centralizes data access, analysis, and number-crunching reduction at your data.  This is a good idea because it saves you, the people in your lab, and whoever else you want, the trouble of downloading the raw data, while still letting you _apply\_your\_own\_analysis_ .  Most of the time, even if you're interested in the whole dataset (or simulation run), you are interested in summary statistics, such as yearly/monthly/daily means, minima and maxima, standard deviations, and such.  In those cases, you can save yourself time by downloading results instead of the raw data.  And since it understands shell-scripts of [NCO](http://nco.sourceforge.net) tools you may already be using, you might already know how to use it to specify complex data processing and analyses.  Perhaps you're worried that your poor data server will be unable to cope?  Fear not.  Unless your data server is far inferior to your workstation, the whole process will probably still feel faster.


## So, really, what is it? (Sysadmin-version) ##
SWAMP is a software package that provides lightweight workflow processing service through a SOAP interface.  A client (bundled, or available separately) provides simple access to the service with syntax mimicking POSIX shell script syntax.  Scripts are submitted to the service, where they are compiled and dynamically targeted for parallel execution (where available).  Security is assured by limiting the available executables to NCO operators and a few common 'shell-helper' executables.  SWAMP always calls executables directly by their full canonical path names, instead of permitting more dangerous subshell evaluation. Cluster-level parallelism is supported in [Grid Engine](http://gridengine.sunsource.net/)-based scheduling scheduling environments (we need test hardware for other systems).  SWAMP is able to optimize execution of your scripts so they run more efficiently on your data server and especially today's modern multi-core processors and cluster computing hardware.  A SWAMP service may increase system workload, though we expect the actual increases to be manageable and commensurate with overall increased data utilization from users, rather than the intensity of computation, which is typically light.

## Is development still going? ##
In 2008, I had the intention of continuing SWAMP development at least in maintenance mode. It is now April 2011, and I haven't made any code changes since late 2008.  I think SWAMP's idea is still very much valid, but I have realized that there are better ways of implementing some of its parts.  The work dispatcher should be using a distributed dispatch library rather than my own code, and the parser should probably use ANTLR grammar that is more powerful (though less readable) so that the supported syntax can expand and be formalized. Therefore, future development, time/funding pending, would focus on this rewrite, rather than tacking on features to the old tree. (Daniel L. Wang on 2011-04-20)

# News #
2011-04-20: Migration of source repository to hg from svn.

2008-05-22: SWAMP presentation at [CCGRID08](http://www.ens-lyon.fr/LIP/RESO/ccgrid2008/) (See [Publications](http://code.google.com/p/swamp/wiki/Publications))

2007-12-10: [SWAMP poster](http://dust.ess.uci.edu/~wangd/pub/wangd_agu2007.pdf) at 2007 American Geophysical Union Fall Meeting: Monday Dec 10. Number IN11B-0469

2007-12-07: SWAMP 0.1 beta client has been released.

# Going further #
  * The [wiki index](http://code.google.com/p/swamp/w/list) has a list of both user documentation and developer notes.
  * The [publications wiki page](http://code.google.com/p/swamp/wiki/Publications) should have a list of SWAMP-related publications.
  * [This figure](http://dust.ess.uci.edu/~wangd/pub/swamp_2007summary.pdf) provides the highlights of SWAMP's progress in 2007.


# Credits #
Work on SWAMP is supported by the [National Science Foundation (NSF)](http://www.nsf.gov) through grant [NSF IIS-0431203](http://www.nsf.gov/awardsearch/showAward.do?AwardNumber=0431203) : "SEI(GEO): Scientific Data Operators Optimized for Efficient Distributed Interactive and Batch Analysis of Tera-Scale Geophysical Data".  SWAMP began as an offshoot of the [netCDF Operators (NCO)](http://nco.sourceforge.net) project funded by this grant.