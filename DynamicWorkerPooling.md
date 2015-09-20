# Introduction #
Dynamic Worker Pooling is a feature that allows a SWAMP master to accept registration from worker instances.  This should reduce the hassle in configuring a SWAMP installation, as well as reduce configuration-change motivated server restarts.  The system should become more stable in general.

Instead of hand-coding the specification of workers in the master's configuration file, we have the workers register themselves with the master.

# Motivation #

As discussed above, a big win from this implementation is the reduction of server configuration hassle.  Another big win, which is less user-visible, is the paving of the path to letting the master adjust its online worker pool dynamically.  This eases integration with existing clusters.

# Details #

When the feature stabilizes, we'll put a better description here.

# Design Strategy #
  * Our first cut will work in this fashion:
    * Master starts up, runs in local-exec mode (or queues jobs indefinitely)
    * A worker starts up:  In doing so, it is preconfigured (alternatively, it could be ordered) to 'phone home'.
    * The worker sends a registration message to the master, including its capabilities and its url.
    * The master responds with "reg ok" or "general failure".
    * The master may now distribute jobs to the worker.
    * When the master is finished, it can send a 'dismiss' command to the worker, which, in the gridengine case, will terminate the worker process.
    * If the worker is started up, but not registered with a master, it should retry every x seconds and commit seppuku after a timeout.

# Milestones #
Milestone 1: Workers register with their masters and are properly added as workers.

Milestone 2: Master keeps a list of available workers: perhaps a directory.  As available, the master spawns the workers via a grid launching command.

# History #
In its original implementation, workers (slaves) were linked to the master process by specifying their URLs in the master's configuration file.  The workers were assumed to have certain capabilities and characteristics, and the master, trusting these characteristics blindly, sent work via the configured URLs, trusting in correct results.




