# Introduction #

Umnberto wants to use SWAMP with his local clustering environment, but his cluster administrators refuse to let him access the cluster in any way except the standard batch submission routines.  The solution?  We implement a mechanism which allows SWAMP to manage its worker pool using a standard cluster batch scheduling system.

# Details #

## Milestone 1 ##
> Our first milestone will utilize an "as simple as possible" strategy.
  * Master spawns workers, one at a time via qsub.
  * All info for starting each worker is packaged in a per-worker unit.
  * In the dynamic case, master spawns new workers if there are commands ready, but no free workers, up to a maximum spawn rate.

## Questions ##
What sort of (configuration) information is needed for the master to perform the spawning?
> - A "low water mark"-- some sort of heuristic or other rule that signals the need for more workers.  Here is a simple rule:
```
if len(readylist) > len(idleworkers) and not-spawned-recently then spawnmore()
```
For now, we spawn one worker at a time. not-spawned-recently exists to minimize chances of a 'spawning storm'.

> - Parameters for cluster spawning: How many compute nodes may we request?  How many processors per node can we request? Which queue do we use?  We would like to package this up so that an administrator can easily do this for the site, with minimal code.


# History #
Our first multicomputer processing implementation of SWAMP required manual startup and shutdown of the master and worker nodes.  With DynamicWorkerPooling, administrators can add and remove workers from master pools by simply starting and shutting down workers, eliminating a disruptive master restart.  The next step is to delegate worker startup and shutdown to a batch scheduler, allowing the number of SWAMP workers to vary dynamically.

## SGE Notes ##
Because workers instances are (nearly) fully-fledged instances of SWAMP services, they have equivalent software requirements as SWAMP itself.  Compute nodes will therefore need:
  * Python 2.4+
  * SOAPpy, a SOAP python library
  * Twisted, a networking framework for python: We use Twisted Web 2.0+(?)
  * NCO
  * netCDF (a prerequisite of NCO)
  * (deprecated) SQLite 3.x, pysqlite2

## Rocks Notes ##
Our test installation is Rocks 4.2, so here are some notes for getting things working in that sort of clustered environment.

A number of non-default RPMs required:
  * python24 (installed separately in /usr/bin/python2.4 separately from default /usr/bin/python)
  * python24-soappy
  * python24-fpconst (required by python24-soappy)
  * python-twisted-core (x86\_64), python-24-twisted-web (noarch)
  * python24-pyopenssl, python24-zope-interface (needed by twisted)

Build these (if needed) and place them in the `/home/install/contrib/${rocks-version}/${arch}/RPMS` directory, adding them to the customization kickstart file at `/home/install/site-profiles/${rocks-version}/nodes/extend-compute.xml` .  For us, `${rocks-version} ` was 4.2 and `${arch} ` was x86\_64.