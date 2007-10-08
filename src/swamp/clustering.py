# $Id: $
"""
sample-SiteCluster - (SAMPLE) customization for a site.
   Rename as "SiteCluster.py" and place in the working directory of the
   SWAMP service.

"""

# 
# Copyright (c) 2007 Daniel Wang, Charles S. Zender
# This sample file is part of SWAMP.
# SWAMP is released under the GNU General Public License version 3 (GPLv3)
#
# Your *customized* site-cluster.py file need not be released under
# GPL.  However, we welcome submissions and contributions as they
# would help others configure their sites. 



# Standard Python imports
#import string
#import os
#import sys
#import time

# (semi-) third-party imports
#import twisted.web.resource as tResource

# SWAMP imports
#import swamp

from itertools import imap, chain
from subprocess import Popen, PIPE, STDOUT
from tempfile import mkstemp
import os

class Cluster:

    @staticmethod
    def makeConfigString():
        """for now, formats a config file for use with a worker.
        """
        # vars needed:
        # log location: /tmp/swampWorker.$pid.log
        overrides = {"log" : {"location" : "/tmp/swampWorker.$pid.log"},
                     "exec" : {"resultPath" : "per-node",
                               "bulkPath" : "per-node",
                               "sourcePath" : "per-node",
                               "localSlots" : "automagic"},
                     "service" : {"hostname" : "automagic",
                                  "port" : "configured(firewallissue)",
                                  "soapPath" : "doesn'tmatter",
                                  "pubPath" : "doesn'tmatter",
                                  "mode" : "worker",
                                  "masterUrl" : "master-filled automagic",
                                  "masterAuth" : "master-filled"}
                     }
        def makeSection(item):
            #print "section",item,item[1].items(),
            return "[%s]\n"%item[0] + "\n".join(map(lambda x: "%s=%s" % x, item[1].items()))
        #print "items=",overrides.items()
        return "\n".join(["#Automagic swamp worker override file (DO NOT EDIT)"]
                         + map(makeSection, overrides.items()))


        
    
    @staticmethod
    def spawnNode():

        options = ["-S /bin/sh", # crucial in our tests
                   "-cwd", # alternatively, add 'cd' cmd to set workingdir
                   "-N swampworker",
                   "-j y",
                   "-o swampworkers.log",
                   "-now y",
                   "-pe swamp 4", # #choose swamp queue, 4 slots.
                   
                   ]
        pre = ["echo Starting ",#"cd /home/wangd/swamptest/sge_play"
               ]
        
        invocation = "python2.4 workerServer.py"
        post = ["echo Finishing"]
        # null element in list exists to force "\n".join to insert a trailing \n
        scriptlines = chain(["#!/bin/sh"],
                            imap(lambda x: "#$ "+x, options),
                            pre, [invocation], post,[""])
        
        (fd, sname) = mkstemp()
        os.write(fd, "\n".join(scriptlines))
        os.close(fd)
        print "wrote",sname
        commandline = "qsub %s" % (sname)
        try:
            output = Popen(commandline.split(),
                           stdout=PIPE).communicate()[0]
            print "called and got output:", output
            if "has been submitted" not in output:
                print "wasn't successful submitting."
        finally:
            pass
            os.unlink(sname)
            #print "skipping unlink of ",sname

    @staticmethod
    def makeCommandlineList(line):
        l = line.split()
        

#
# current strategy:
# when spawning workerserver spawn with:
# python2.4 workerServer.py -c baseWorker.conf -c workers.conf:hostname
# colon in hostname selects a section in the file.
# How to specify config for each node?
# -- Directory of node-specific config files?  clumsy for lots of nodes
# -- SiteConfig file: define a method: config(hostname) that returns
#    a well-defined config file(?) for a given hostname.  Flexible, but
#    we're making people write code.... hopefully it's not too hard.
#
#  Where to specify config:
#  Master conf file -- natural place for master config
#  Worker template conf -- another place?
#  Smart config (python) -- make people write code?


############################################################

# SGE Qsub documentation:
# qsub [ options ] [ command | -- [ command_args ]]
#      Qsub submits batch jobs to the Grid Engine queuing system. Grid Engine
#        supports single- and multiple-node jobs. Command can be a  path  to  a
#        binary  or  a  script (see -b below) which contains the commands to be
#        run by the job using a shell (for example, sh(1)  or  csh(1)).   Argu-
#        ments  to  the command are given as command_args to qsub .  If command
#        is handled as a script then it is  possible  to  embed  flags  in  the
#        script.   If  the  first  two characters of a script line either match
#        '#$' or are equal to the prefix string  defined  with  the  -C  option
#        described below, the line is parsed for embedded command flags.
# relevant options:
#       -@ optionfile
#               Forces  qsub, qrsh, qsh, or qlogin to use the options contained
#               in  optionfile.  The  indicated  file  may  contain  all  valid
#               options. Comment lines must start with a "#" sign.

#        -ac variable[=value],...
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               Adds the given name/value pair(s) to the job's  context.  Value
#       -A account_string
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               Identifies the account to which the resource consumption of the
#               job  should be charged. The account_string may be any arbitrary
#               ASCII alphanumeric string but  may  not  contain   "\n",  "\t",
#               "\r", "/", ":", "@", "\", "*",  or "?".  In the absence of this
#               parameter Grid Engine will place  the  default  account  string
#               "sge" in the accounting record of the job.
#       -cwd   Available for qsub, qsh, qrsh and qalter only.

#               Execute the job  from  the  current  working  directory.   This
#               switch  will  activate Grid Engine's path aliasing facility, if
#               the  corresponding  configuration  files   are   present   (see
#               sge_aliases(5)).
#       -e [[hostname]:]path,...
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               Defines  or  redefines  the  path  used  for the standard error
#               stream of the job. For qsh, qrsh and qlogin only  the  standard
#               error  stream  of prolog and epilog is redirected.  If the path
#               constitutes an absolute path name, the error-path attribute  of
#               the  job  is  set  to path, including the hostname. If the path
#               name is relative, Grid Engine expands path either with the cur-
#               rent  working directory path (if the -cwd switch (see above) is
#               also specified) or with the home directory path. If hostname is
#               present, the standard error stream will be placed in the corre-
#               sponding location only if the job runs on the  specified  host.
#               If  the  path  contains a ":" without a hostname, a leading ":"
#               has to be specified.

#               By default the file name for interactive jobs is /dev/null. For
#               batch  jobs the default file name has the form job_name.ejob_id
#               and job_name.ejob_id.task_id for array job tasks (see -t option
#               below).

#               If  path  is  a directory, the standard error stream of the job
#               will be put in this directory under the default file name.   If
#               the  pathname  contains  certain  pseudo environment variables,
#               their value will be expanded at runtime of the job and will  be
#               used  to  constitute  the  standard error stream path name. The
#               following pseudo environment variables are supported currently:

#               $HOME       home directory on execution machine
#               $USER       user ID of job owner
#               $JOB_ID     current job ID
#               $JOB_NAME   current job name (see -N option)
#               $HOSTNAME   name of the execution host
#               $TASK_ID    array job task index number

#               Alternatively to $HOME the tilde sign "~" can be used as common
#               in csh(1) or ksh(1).  Note, that the "~"  sign  also  works  in
#               combination  with  user names, so that "~<user>" expands to the
#               home directory of <user>. Using another user ID  than  that  of
#               the job owner requires corresponding permissions, of course.
#        -i [[hostname]:]file,...
#               Available for qsub, and qalter only.

#               Defines or redefines the  file  used  for  the  standard  input
#               stream  of  the  job. If the file constitutes an absolute file-
#               name, the input-path attribute of  the  job  is  set  to  path,
#               including  the  hostname.  If  the  path name is relative, Grid
#               Engine expands path either with the current  working  directory
#               path (if the -cwd switch (see above) is also specified) or with
#               the home directory path. If hostname is present,  the  standard
#               input  stream will be placed in the corresponding location only
#               if the job runs on the specified host. If the path  contains  a
#               ":" without a hostname, a leading ":" has to be specified.

#               By default /dev/null is the input stream for the job.

#               It  is  possible  to use certain pseudo variables, whose values
#               will be expanded at runtime of the job  and  will  be  used  to
#               express the standard input stream as described in the -e option
#               for the standard error stream.

#       -j y[es]|n[o]
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               Specifies whether or not the standard error stream of  the  job
#               is merged into the standard output stream.
#               If  both  the  -j y and the -e options are present, Grid Engine
#               sets but ignores the error-path attribute.

#               Qalter allows changing this option even while the job executes.
#               The  modified  parameter will only be in effect after a restart
#               or migration of the job, however.

#      -now y[es]|n[o]
#               Available for qsub, qsh, qlogin and qrsh.

#               -now  y  tries  to start the job immediately or not at all. The
#               command returns 0 on success, or 1 on failure (also if the  job
#               could  not be scheduled immediately).  For array jobs submitted
#               with the -now option, if all tasks cannot be immediately sched-
#               uled,  no tasks are scheduled.  -now y is default for qsh, qlo-
#               gin and qrsh
#               With the -now n option, the job will be put  into  the  pending
#               queue  if  it cannot be executed immediately. -now n is default
#               for qsub.


#        -N name
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               The name of the job. The name can be any printable set of char-
#               acters  except  "\n",  "\t", "\r", "/", ":", "@", "\", "*", and
#               "?", and it has to start with an alphabetic character.  Invalid
#               job names will be denied at submit time.
#               If  the  -N option is not present, Grid Engine assigns the name
#               of the job script to the job after any directory  pathname  has
#               been  removed  from the script-name. If the script is read from
#               standard input, the job name defaults to STDIN.
#               In the case of qsh or qlogin with the -N option is absent,  the
#               string 'INTERACT' is assigned to the job.
#               In the case of qrsh with the -N option is absent, the resulting
#               job name is determined from the qrsh command line by using  the
#               argument  string  up  to the first occurrence of a semicolon or
#               whitespace and removing the directory pathname.


#       -o [[hostname]:]path,...
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               The  path  used  for the standard output stream of the job. The
#               path is handled as described in the -e option for the  standard
#               error stream.

#               By  default  the  file  name  for  standard output has the form
#               job_name.ojob_id and  job_name.ojob_id.task_id  for  array  job
#               tasks (see -t option below).

#               Qalter allows changing this option even while the job executes.
#               The modified parameter will only be in effect after  a  restart
#               or migration of the job, however.

#       -pe parallel_environment n[-[m]]|[-]m,...
#               Available for qsub, qsh, qrsh, qlogin and qalter only.

#               Parallel programming environment (PE) to instantiate. The range
#               descriptor  behind the PE name specifies the number of parallel
#               processes to be run. Grid Engine will allocate the  appropriate
#               resources  as  available.  The  sge_pe(5)  manual page contains
#               information about the definition of PEs and about how to obtain
#               a list of currently valid PEs.
#               You  can  specify  a PE name which uses the wildcard character,
#               "*".  Thus the request "pvm*" will match any parallel  environ-
#               ment with a name starting with the string "pvm". In the case of
#               multiple parallel  environments  whose  names  match  the  name
#               string,  the parallel environment with the most available slots
#               is chosen.
#               The range specification is a list of range expressions  of  the
#               form "n-m", where n and m are positive, non-zero integers.  The
#               form "n" is equivalent to "n-n".  The form "-m"  is  equivalent
#               to  "1-m".   The  form "n-" is equivalent to "n-infinity".  The
#               range specification is processed as follows: The largest number
#               of  queues requested is checked first. If enough queues meeting
#               the specified attribute list are available, all are  allocated.
#               If  not,  the  next smaller number of queues is checked, and so
#               forth.
#               If additional -l options are present, they restrict the set  of
#               eligible queues for the parallel job.

#               Qalter allows changing this option even while the job executes.
#               The modified parameter will only be in effect after  a  restart
#               or migration of the job, however.

#       -shell y[es]|n[o]
#               Available only for qsub.

#               -shell n causes qsub to execute the command line  directly,  as
#               if  by exec(2).  No command shell will be executed for the job.
#               This option only applies when -b y is also used.  Without -b y,
#               -shell n has no effect.

#               This option can be used to speed up execution as some overhead,
#               like the shell startup and sourcing the shell resource files is
#               avoided.

#               This  option can only be used if no shell-specific command line
#               parsing is required. If the command line contains shell syntax,
#               like  environment  variable  substitution  or (back) quoting, a
#               shell must be started.  In this case  either  do  not  use  the
#               -shell  n  option  or execute the shell as the command line and
#               pass the path to the executable as a parameter.

#               If a job executed with the -shell n option fails due to a  user
#               error,  such as an invalid path to the executable, the job will
#               enter the error state.
# :      -sync y[es]|n[o]
#               Available for qsub.

#               -sync  y  causes  qsub  to  wait for the job to complete before
#               exiting.  If the job completes successfully, qsub's  exit  code
#               will  be  that  of the completed job.  If the job fails to com-
#               plete successfully, qsub will print out a error  message  indi-
#               cating  why the job failed and will have an exit code of 1.  If
#               qsub is interrupted, e.g. with  CTRL-C,  before  the  job  com-
#               pletes, the job will be canceled.
#               With  the -sync n option, qsub will exit with an exit code of 0
#               as soon as the job  is  submitted  successfully.   -sync  n  is
#               default for qsub.
#               If -sync y is used in conjunction with -now y, qsub will behave
#               as though only -now y were given until the job  has  been  suc-
#               cessfully  scheduled,  after  which  time  qsub  will behave as
#               though only -sync y were given.
#               If -sync y is used in conjunction with -t n[-m[:i]], qsub  will
#               wait  for  all  the job's tasks to complete before exiting.  If
#               all the job's tasks complete  successfully,  qsub's  exit  code
#               will  be  that of the first completed job tasks with a non-zero
#               exit code, or 0 if all job tasks exited with an exit code of 0.
#               If  any  of the job's tasks fail to complete successfully, qsub
# :
#        -v variable[=value],...
#               Available for qsub, qsh, qrsh and qalter.

#               Defines  or  redefines the environment variables to be exported
#               to the execution context of the  job.   If  the  -v  option  is
#               present  Grid Engine will add the environment variables defined
#               as arguments to the switch and, optionally, values of specified
#               variables, to the execution context of the job.

#        -verify
#               Available for qsub, qsh, qrsh, qlogin and qalter.

#               Instead of submitting a job, prints detailed information  about
#               the would-be job as though qstat(1) -j were used, including the
#               effects of command-line parameters and  the  external  environ-
#               ment.

#      -V     Available  for qsub, qsh, qrsh with command, qalter and qresub.

#               Specifies that all environment variables active within the qsub
#               utility be exported to the context of the job.

#        SGE_ROOT       Specifies the location of the Grid Engine standard con-
#                       figuration files.

#        SGE_CELL       If set, specifies the  default  Grid  Engine  cell.  To
#                       address  a Grid Engine cell qsub, qsh, qlogin or qalter
#                       use (in the order of precedence):

#                              The name of the cell specified in  the  environ-
#                              ment variable SGE_CELL, if it is set.

#                              The name of the default cell, i.e. default.

#        SGE_DEBUG_LEVEL
#                       If  set,  specifies  that  debug  information should be
#                       written to stderr. In addition the level of  detail  in
#                       which debug information is generated is defined.

#        In addition to those environment variables specified to be exported to
#        the job via the -v or the -V option (see above) qsub, qsh, and  qlogin
#        add  the following variables with the indicated values to the variable
#        list:

#        SGE_O_HOME     the home directory of the submitting client.

#        SGE_O_HOST     the name of the host on which the submitting client  is
#                       running.

#        SGE_O_LOGNAME  the LOGNAME of the submitting client.

#        SGE_O_MAIL     the  MAIL  of  the  submitting client. This is the mail
#                       directory of the submitting client.

#        SGE_O_PATH     the executable search path of the submitting client.

#        SGE_O_SHELL    the SHELL of the submitting client.

#        SGE_O_TZ       the time zone of the submitting client.

#        SGE_O_WORKDIR  the absolute path of the current working  directory  of
# :
#       Furthermore,  Grid  Engine  sets  additional  variables into the job's
#        environment, as listed below.

#        ARC

#        SGE_ARCH       The Grid Engine architecture name of the node on  which
#                       the  job  is  running. The name is compiled-in into the
#                       sge_execd(8) binary.

#        SGE_CKPT_ENV   Specifies the checkpointing  environment  (as  selected
#                       with  the -ckpt option) under which a checkpointing job
#                       executes. Only set for checkpointing jobs.

#        SGE_CKPT_DIR   Only set for checkpointing jobs. Contains path ckpt_dir
#                       (see checkpoint(5) ) of the checkpoint interface.

#        SGE_STDERR_PATH
#                       the  pathname  of  the file to which the standard error
#                       stream of  the  job  is  diverted.  Commonly  used  for
#                       enhancing  the  output with error messages from prolog,
#                       epilog, parallel environment start/stop or  checkpoint-
# :
#       ENVIRONMENT    The ENVIRONMENT variable is set to  BATCH  to  identify
#                       that  the  job is being executed under Grid Engine con-
#                       trol.

#        HOME           The user's home directory path from the passwd(5) file.

#        HOSTNAME       The hostname of the node on which the job is running.

#        JOB_ID         A unique identifier assigned by the sge_qmaster(8) when
#                       the job was submitted. The job ID is a decimal  integer
#                       in the range 1 to 99999.

#        JOB_NAME       The job name.  For batch jobs or jobs submitted by qrsh
#                       with a command, the job name is built  as  basename  of
#                       the  qsub  script filename resp. the qrsh command.  For
#                       interactive jobs it is set  to  'INTERACTIVE'  for  qsh
#                       jobs,  'QLOGIN'  for qlogin jobs and 'QRLOGIN' for qrsh
#                       jobs without a command.

#                       This default may be overwritten by the -N.  option.

#        LOGNAME        The user's login name from the passwd(5) file.
#        NHOSTS         The number of hosts in use by a parallel job.

#        NQUEUES        The number of queues allocated for the  job  (always  1
#                       for serial jobs).

#        NSLOTS         The number of queue slots in use by a parallel job.

#        PATH           A default shell search path of:
#                       /usr/local/bin:/usr/ucb:/bin:/usr/bin

#        SGE_BINARY_PATH
#                       The  path where the Grid Engine binaries are installed.
#                       The value is the concatenation of the cluster  configu-
#                       ration  value  binary_path  and  the  architecture name
#                       $SGE_ARCH environment variable.

#        PE             The parallel environment under which the  job  executes
#                       (for parallel jobs only).

#        PE_HOSTFILE    The  path  of  a  file containing the definition of the
#                       virtual parallel machine assigned to a parallel job  by
#                       Grid  Engine.  See  the description of the $pe_hostfile
#                       parameter in sge_pe(5) for details  on  the  format  of
# 
#        QUEUE          The name of the cluster queue in which the job is  run-
#                       ning.

#        REQUEST        Available for batch jobs only.

#                       The  request  name  of  a  job as specified with the -N
#                       switch (see above) or taken as  the  name  of  the  job
#                       script file.

#        RESTARTED      This variable is set to 1 if a job was restarted either
#                       after a system crash or after a migration in case of  a
#                       checkpointing  job. The variable has the value 0 other-
#                       wise.

#        SHELL          The user's login shell from the passwd(5)  file.  Note:
#                       This is not necessarily the shell in use for the job.

#        TMPDIR         The absolute path to the job's temporary working direc-
#                       tory.

#        TMP            The same as TMPDIR;  provided  for  compatibility  with
#                       NQS.

#        USER           The user's login name from the passwd(5) file.



def main():
    print Cluster.makeConfigString()
    #Cluster.spawnNode()

if __name__ == "__main__":
    main()
