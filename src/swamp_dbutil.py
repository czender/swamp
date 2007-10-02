#!/usr/bin/env python
       
# $Id$
#
# This is:  -- a module for managing state persistence for the dap handler.
#           -- Uses a SQLite backend.
#
# 
# The future of SWAMP's db-backed backend is unclear. Heavy contention
# in a SQLite db with multiple readers/writers many times a second
# (dozens? hundreds?) was difficult on physical disk, despite RAID or
# caching, and ramdisk version (not really persistent) was required.
# DB persistence is useful to have server state survive restarts, but
# it's empirically clear that persistance is not practical unless
# updates are kept below a frequency of, say, 1Hz.
#
# With this in mind, this code is currently kept only for history.
# 

import os, sys, time

try:
    from pysqlite2 import dbapi2 as sqlite
except Exception, e:
    pass # silence import errors.


class local:
    @staticmethod
    def defaultLogit(s):
        print "dbutil:", s

    @staticmethod
    def setLogger():
        """setLogger()-- via some Python introspection,
        search the calling stack to find dap_nc_swamp's logging facility."""
        import inspect
        s = inspect.stack()
        global logit
        for c in s: # for each context in the call stack...
            if "logit" in c[0].f_locals: # first in tuple is frame
                logit = c[0].f_locals["logit"]
        try:
            if logit is None:
                logit = local.defaultLogit
                #print >>open("/tmp/foo1","a"), dir(local)
        except:
            logit = local.defaultLogit
        logit("logger initialized")
local.setLogger()


class JobPersistence:
    defaultDbFilename = "/tmp/mydb_ssdap"
    fileStateMap = {1 : "planned",
                    2 : "running",
                    3 : "saved",
                    4 : "removed"
                    }
    class Transaction:
        def executeBlocking(self, *pargs, **kwargs):
            """This keeps trying an operation until it succeeds.
            Transient DB exceptions are explicitly caught."""
            val = None
            while True:
                try:
                    val =  self.execute(*pargs, **kwargs)
                    break
                except sqlite.OperationalError, e:
                    # if db is locked, wait and retry.
                    if 'database is locked' in str(e):
                        #print >>open("/tmp/foo1","a"), os.getpid(),"dbretry(lock)"
                        time.sleep(0.5)
                        continue
                    elif 'SQL statements in progress' in str(e):
                        self.cursor.execute("ROLLBACK;")
                        self.cursor.close()
                        #print >>open("/tmp/foo1","a"), os.getpid(),"dbretry(inprogress)", pargs
                        time.sleep(0.5)
                        continue                        
                    else: #otherwise, pass the exception upwards.
                        raise
                pass
            if "postExecute" in dir(self):
                # run post-transactional execute stuff
                self.postExecute()
            return val
            pass
        pass # end of Transaction class
    class PopulationTransaction:
        """An interface for the task of loading the db from a script parsing
        run.  Don't forget to call finish() when you're done!"""
        def __init__(self, connection, taskRowCallback = lambda x: None):
            """An interface for initial population of the db from a script
            Precondition: connection has no outstanding commands
            or transactions.  Only one task can be tracked.
            * overwrites connection.isolation_level, so save it if you need it.
            taskRowFunction is a unary function which will be called with
            the taskRow, once it is known
            """
            # setup some variables
            self.connection = connection
            self.taskId = None
            self.taskRow = None
            self.callbackTaskRow = taskRowCallback
            self.cmdList = []
            self.inOutList = []
            self.stateList = [] # cache inserts to batch until commit

            # setup the cursor: do manual transaction management.
            connection.isolation_level = None
            pass

        def insertTask(self, taskid):
            """inserts a task into the right tables,
            returns resultant rownumber for use in other tables"""
            #sanity:
            if self.taskId is not None or self.taskRow is not None:
                class StateError(Exception):
                    def __str__(self):
                        return repr("Already inserted Task; cannot re-insert")
                raise StateError()
            
            sqlcmd = """BEGIN IMMEDIATE;
            INSERT INTO tasks (taskid, date) VALUES ('%s','%s');
            COMMIT;"""
            sqlcmd2 = "INSERT INTO tasks (taskid, date) VALUES ('%s','%s');"
             # today = "%04d%02d%02d" % time.localtime()[:3]
            # date in yyyy-mm-dd hh:mm format
            today = "%04d-%02d-%02d %02d:%02d" % time.localtime()[:5]
            cur = self.connection.cursor()
            #logit(sqlcmd2 % (taskid, today))
            tries = 0
            try:
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(sqlcmd2 % (taskid, today))
                cur.execute("COMMIT;")
            except:
                tries += 1
                if tries > 20:
                    raise "Too busy: DB is too busy to accept updates right now"
                while True:
                    time.sleep(0.2) # sleep for a little bit. MAGIC#
                    try:
                        cur.execute("COMMIT;")
                        break
                    except:
                        #print >>open("/tmp/foo1","a"), os.getpid(),"exception2", traceback.format_exc()

                        pass
            
            sqlcmd = """SELECT rowid FROM tasks WHERE taskid=\"%s\";"""
            cur.execute("BEGIN;")
            cur.execute(sqlcmd % (taskid))
            row = cur.fetchall()
            cur.execute("END;")
            cur.close()
            rownum = row[0][0]
            assert len(row) == 1
            self.taskId = taskid
            self.taskRow = rownum
            #print >>sys.stderr, "inserted task in db (uncommit)"
            return rownum 
        def insertCmd(self, linenum, cmd, cmdline):
            """Inserts a command (i.e. a script line) into the right tables"""
            assert self.taskRow is not None
            self.cmdList.append((self.taskRow, linenum, str(cmd), cmdline))
            pass
        def commitCmds(self):
            """Commit whatever command entries are currently queued.
            Shouldn't need to be called by client code.  Might make this a
            'private' method."""

            if(len(self.cmdList) > 0):
                cur = self.connection.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.executemany("""INSERT INTO cmds (taskrow, linenum, cmd, cmdline)
                values (?,?,?,?)""", self.cmdList)
                cur.execute("COMMIT;")
                cur.close()
            self.cmdList = []
            pass
                        
        def insertInOut(self, linenum, logical, concrete, output, state, isTemp):
            """insert a tuple in the inout table.
            cmdid -- id of relevant command
            logical -- logical filename
            concrete -- concrete-mapped filename
            output -- True if this is an output, False, otherwise
            state -- filestate (planned(1), saved-needed(2),
                                saved-deletable(3), removed(4)

            returns a token to allow later retrieval of output file
            """
            fileid = None
            assert self.taskRow is not None
            outnum = [0,1][output == True]
            self.inOutList.append((self.taskRow, linenum, outnum,
                                   str(logical), str(concrete)))
            #defer this to a batch insert right before committing.
            if output: # insert into the token table if it's an output
                if not isTemp: # only insert real files now, defer temps.
                    # skipping 'select' of temp fileids
                    # performance of  persistCommand from 64s to 6s
                    cur = self.connection.cursor()
                    cur.execute("BEGIN IMMEDIATE;")
                    cur.execute("INSERT INTO fileState VALUES(?,?,?);",
                                (self.taskId, concrete, state))
                    cur.execute("COMMIT;")
                    cur.execute("BEGIN;")
                    sql = """SELECT rowid FROM fileState
                    WHERE concretename='%s';"""
                    cur.execute(sql % concrete)
                    row = cur.fetchall()
                    fileid = row[0][0]
                    cur.execute("COMMIT;")
                    cur.close()
                else:
                    self.stateList.append((self.taskId, str(concrete), state))
            return fileid
        def insertInOutDefer(self, linenum, logical, concrete,
                             output, state):
            """an "always-defer" version of insertInOut.  Logical names
            have become canonical, since we assume we'll have
            file-renaming code. to prevent ambiguity.
            Also, we're deferring concrete name assignment until run-time,
            to facilitate work delegation.
            """
            assert self.taskRow is not None
            outnum = [0,1][output == True]
            self.inOutList.append((self.taskRow, linenum, outnum,
                                   str(logical), str(concrete)))
            if output:
                self.stateList.append((self.taskId, str(concrete), state))
            return None            
        def commitInOuts(self):
            """Commit those inserts that were queued earlier during insertInOut
            Shouldn't need to be called by client code.  Might make this a
            'private' method."""
            
            substTemp = """INSERT INTO cmdFileRelation
            (taskrow, linenum, output, logicalname, concretename)
            VALUES (?,?,?,?,?)"""
            cur = self.connection.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            #print "inout commit has %d inout and %d state to commit" % (
            #    len(self.inOutList), len(self.stateList))
            if(len(self.inOutList) > 0):
                cur.executemany(substTemp, self.inOutList)
                self.inOutList = []
            if(len(self.stateList) > 0):
                cur.executemany("""INSERT INTO filestate (taskId, concretename, state)
                values (?,?,?)""", self.stateList)
                self.stateList = []
                cur.execute("COMMIT;")
                cur.close()
            pass
        def finish(self):
            """don't forget to call this to close things out."""
            self.commitCmds()
            self.commitInOuts()
            self.callbackTaskRow(self.taskRow)
            self.taskId = None
            self.taskRow = None
            pass
    class PreparationTransaction:
        """An interface for the task of preparing a loaded script for
        execution. """
        def __init__(self, connection, taskRow):
            """ Precondition: connection has no outstanding commands
            or transactions.  Only one task can be tracked.
            * overwrites connection.isolation_level, so save it if you need it.
            """
            # setup some variables
            self.connection = connection
            self.taskRow = taskRow
            # setup the cursor: do manual transaction management.
            connection.isolation_level = None
            pass
        def execute(self):
            """Find all ready jobs and put them on the readylist
            Warning: logic duplication between this and makeReady.
            Refactoring these two is a priority.
            """
            sql = """SELECT linenum,output,concretename,state FROM cmds
            LEFT JOIN cmdFileRelation USING (taskrow,linenum)
            LEFT JOIN fileState USING (concretename)
            WHERE taskrow=%d"""
            cur = self.connection.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute(sql % self.taskRow)
            readyCmds = {}
            f = cur.fetchall()
            c = 0
            for (linenum, output,concretename, state) in f:
                #print "cmdList: %d,%d has %s in %s" % (
                #    taskrow, linenum, concretename, state)
                ready = readyCmds.get(linenum, (True,None))
                if not output: #output is either missing or false(input)
                    if (state is not None) and (state != 3): #if not saved?
                        readyCmds[linenum] = (False,ready[1])
                        continue
                    #        print "Not Ready %d,%d has %s in %s" % (
                    #            taskrow, linenum, concretename, state)
                readyCmds[linenum] = (ready[0],concretename)
                c = c + 1
                #if (c % 1000) == 0:
                    #print >>open("/tmp/foo1","a"), os.getpid(),"initMakeReady",c
                
            i = readyCmds.items()
            # filter for only the true values, and then pick the first
            # half of the tuple
            i = map(lambda x:(self.taskRow,x[0],x[1][1]),
                    filter(lambda x: x[1][0], i))
            i.sort()
            sql = "INSERT INTO readyList VALUES(?,?,?);"
            cur.execute("DELETE FROM readyList WHERE taskrow=?;",
                        (self.taskRow,))
            cur.executemany(sql,i)
            cur.execute("COMMIT;")
            cur.close()
            pass
        
        pass # end of PreparationTransaction class def
    class FetchAndLockTransaction(Transaction):
        """An interface class for a transaction that fetches the next ready
        job, takes it off the list, and returns it"""
        def __init__(self, connection, taskRow):
            """ Precondition: connection has no outstanding commands
            or transactions.  Only one task can be tracked.
            * overwrites connection.isolation_level, so save it if you need it.
            """
            # setup some variables
            self.connection = connection
            self.taskRow = taskRow
            # setup the cursor: do manual transaction management.
            connection.isolation_level = None
            pass
        def execute(self):
            """Fetches the next scripted command for the bound task"""
            cur = self.connection.cursor()
            self.cursor = cur # in case transaction gets rolled back
            cur.execute("BEGIN EXCLUSIVE;")
            #print >>open("/tmp/foo1","a"), os.getpid(),"fetchlock", time.asctime()
            #stime = time.time()

            # fetch a cmd from ready list
            #cmd = """SELECT taskrow,linenum,concretename,cmdLine
            #FROM readyList JOIN cmds
            #USING (taskrow,linenum) WHERE taskrow=? LIMIT 1;"""
            # select most 
            cmd = """SELECT taskrow,linenum,concretename,cmdLine
            FROM readyList JOIN cmds
            USING (taskrow,linenum) WHERE taskrow=?
            ORDER BY linenum LIMIT 1;"""
            rows = None
            con = None
            cur.execute(cmd, (self.taskRow,))
            rows = cur.fetchall()
            if not rows:
                rows = None
                result = None
            elif len(rows) > 0:
                #print >>open("/tmp/foo1","a"), os.getpid(),"got ready:",rows
                mycommand = rows[0]
                # return cmdline and output concretename and linenum
                result = (mycommand[3],mycommand[2], mycommand[1])
                # drop the cmd from the list
                cmd = """DELETE FROM readyList WHERE taskrow=? AND linenum=?;"""
                cur.execute(cmd, (mycommand[0],mycommand[1]))
                # update the filestate to running
                # hope filename doesn't have any quotes!
                cmd = "UPDATE fileState SET state=2 WHERE concretename=?;"
                cur.execute(cmd, (mycommand[2],))

            #etime = time.time()
            #print >>open("/tmp/foo1","a"), os.getpid(),"unlock after", etime-stime
            cur.execute("COMMIT;")
            cur.close()
            return result
            
        pass # end of FetchAndLockTransaction class def
    class CommitCmdResultTransaction(Transaction):
        def __init__(self, connection):
            self.connection = connection
            connection.isolation_level = None
            pass
                
        def execute(self, concretename, cmdlinenum = None):
            cur = self.connection.cursor()
            self.cursor = cur # in case transaction gets rolled back
            cur.execute("BEGIN EXCLUSIVE;")
            #print >>open("/tmp/foo1","a"), os.getpid(),"cmtcmd", time.asctime()
            stime = time.time()
            cur.execute("""UPDATE fileState SET state=3
                           WHERE concretename=?;""", (concretename,))
            # find all commands affected by the committed's output
            cur.execute("""SELECT taskrow,linenum FROM cmdFileRelation
            WHERE output=0 AND concretename=?;""", (concretename,))
            rows = cur.fetchall()

            # find the outputfilename for this command, but only if the count
            # of its inputfiles that are not in the saved state is zero
            cmd = """SELECT concretename FROM cmdFileRelation
            WHERE (SELECT COUNT(*) FROM cmdFileRelation
                                   JOIN fileState USING(concretename)
                                   WHERE taskrow=? AND linenum=?
                                   AND output=0 AND state<>3
                  )=0 AND taskrow=? AND linenum=? AND output=1;"""
            newReady=[]
            for (taskrow,linenum) in rows:
                cur.execute(cmd, (taskrow,linenum,taskrow,linenum))
                ctuple = cur.fetchall()
                if len(ctuple) == 1:
                    newReady.append((taskrow, linenum, ctuple[0][0]))
            readyTemplate = """INSERT INTO readyList
            (taskrow,linenum,concretename) VALUES (?,?,?)"""
            if len(newReady) > 0:
                cur.executemany(readyTemplate, newReady)
            cur.execute("COMMIT;")
            etime = time.time()
            #print >>open("/tmp/foo1","a"), os.getpid(),"unlock after", etime-stime
            cur.close()
            pass
        pass # end of CommitCmdResultTransaction class def
    class CommitAndFetchTransaction(Transaction):
        """A combo transaction that commits a command and returns the first cmd
        made ready as a result, if it exists.  If one exists, this saves
        a db transaction to fetch the next ready command.
        -- correct usage:
         either through executeBlocking or execute/postExecute sequence
        """
        def __init__(self, connection):
            self.connection = connection
            connection.isolation_level = None
            self.cursor = None
            pass
        def execute(self, concretename, inputlist):
            """ concretename : concrete filename that is being finished
                inputlist : list of tuples of (file, count), consisting of
                            files and counts of how many times they should
                            be used before being deleted."""
            cur = self.connection.cursor() 
            self.cursor = cur # in case transaction gets rolled back, etc.
            cur.execute("BEGIN EXCLUSIVE;")
            #print >>open("/tmp/foo1","a"), os.getpid(),"cmtfch", time.asctime()
            stime = time.time()
            cur.execute("""UPDATE fileState SET state=3
                           WHERE concretename=?;""", (concretename,))
            # find all commands affected by the committed's output
            cur.execute("""SELECT taskrow,linenum FROM cmdFileRelation
            WHERE output=0 AND concretename=?;""", (concretename,))
            rows = cur.fetchall()

            # find the outputfilename for this command, but only if the count
            # of its inputfiles that are not in the saved state is zero
            cmd = """SELECT concretename FROM cmdFileRelation
            WHERE (SELECT COUNT(*) FROM cmdFileRelation
                                   JOIN fileState USING(concretename)
                                   WHERE taskrow=? AND linenum=?
                                   AND output=0 AND state<>3
                  )=0 AND taskrow=? AND linenum=? AND output=1;"""
            newReady=[]
            for (taskrow,linenum) in rows:
                cur.execute(cmd, (taskrow,linenum,taskrow,linenum))
                ctuple = cur.fetchall()
                if len(ctuple) == 1:
                    newReady.append((taskrow, linenum, ctuple[0][0]))
            readyTemplate = """INSERT INTO readyList
            (taskrow,linenum,concretename) VALUES (?,?,?)"""
            result = None
            if len(newReady) > 0:
                # steal the first cmd and pre-"fetch-and-lock" it
                fetchedCmd = newReady.pop(0)
                result = self.markStart(fetchedCmd)
                if len(newReady) > 0:  # now insert if there's more.
                    cur.executemany(readyTemplate, newReady)
            self.updateDeleteTracker(inputlist)
            cur.execute("COMMIT;")
            etime = time.time()
            #print >>open("/tmp/foo1","a"), os.getpid(),"unlock after", etime-stime
            cur.close()
            return result
        def markStart(self, readyTuple):
            """helper for the case where a new ready job exists."""
            (taskrow, linenum, concretename) = readyTuple
            fetch = "SELECT cmdLine FROM cmds WHERE taskrow=? AND linenum=?;"
            update = "UPDATE fileState SET state=2 WHERE concretename=?;"
            self.cursor.execute(fetch, (taskrow, linenum))
            rows = self.cursor.fetchall()
            assert len(rows) == 1
            result = (rows[0][0], concretename, linenum)
            self.cursor.execute(update, (concretename,))
            return result
        def updateDeleteTracker(self, inputlist):
            """inputlist is a list of tuples of (concretename, usagecount)
            where usagecount is the number of times this file is consumed during
            the dataflow
            """
            fetch = "SELECT count FROM useList WHERE concretename=?;"
            deleteList = []
            updateList = []
            setList = []
            for (concretename, count) in inputlist:
                self.cursor.execute(fetch, (concretename,))
                rows = self.cursor.fetchall()
                if len(rows) == 0:
                    if count == 1: # only supposed to be used once, ok to del
                        deleteList.append((concretename,))
                    else:
                        setList.append((concretename, 1))
                else:
                    assert len(rows) == 1
                    curcount = int(rows[0][0]) + 1 # increment counter
                    if curcount == count: # ok to delete
                        #print >>open("/tmp/foo1","a"), "ok to del",concretename
                        deleteList.append((concretename,))
                    else: 
                        updateList.append((curcount, concretename))
            # now, apply updates and deletes to list
            update = "UPDATE useList SET count=? WHERE concretename=?"
            self.cursor.executemany(update, updateList)
            delete = "DELETE FROM useList WHERE concretename=?"
            self.cursor.executemany(delete, deleteList)
            set = "INSERT INTO useList VALUES(?,?);"
            self.cursor.executemany(set, setList)
            # defer real deletes to occur outside the transaction.
            self.deleteList = deleteList
            pass
        def postExecute(self):
            """after transaction completes, process deferred behavior.
            --delete queued files."""
            #if "deleteList" not in dir(self): # should always be true
            #    return
            for f in self.deleteList:
                try:
                    #print "unlinking f", f[0]
                    #print >>open("/tmp/foo1","a"), os.getpid(),"try del", f[0]
                    os.unlink(f[0])
                except OSError,e:
                    logit("error deleting %s"%(f[0]))
                    # log error... 
            pass
        
    class PollingTransaction:
        def __init__(self, connection, taskRow):
            assert type(connection) == sqlite.Connection
            self.connection = connection
            if taskRow is not None:
                self.taskRow = taskRow
                assert type(taskRow) == int
            # setup the cursor: do manual transaction management.
            connection.isolation_level = None

        def cmdsLeft(self):
            """Checks to see if there are any cmds left to exec.
            Check by seeing if there are any more output files that are not
            running or saved or removed.
            """
            sql = """SELECT COUNT(state) FROM cmds
            JOIN cmdFileRelation USING (taskrow,linenum)
            JOIN fileState USING (concretename)
            WHERE taskrow=? AND output=1 AND state=1;"""
            cur = self.connection.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute(sql,(self.taskRow,))
            result = cur.fetchall()[0][0]
            cur.execute("COMMIT;")
            cur.close()
            return result
        def pollFileStateById(self, id):
            """Check the state of a file with the supplied id
            Returns: the state of the file, if it exists."""
            cur = self.connection.cursor()
            cur.execute("BEGIN;")
            cur.execute("SELECT state FROM filestate WHERE rowid=?;",
                        (id,))
            states = cur.fetchall()
            result = None
            if states is not None and len(states) == 1:
                result = int(states[0][0])
            cur.execute("COMMIT;")
            cur.close()
            return result
        def pollFileStateByLogical(self, logical):
            """Check the state of a file with the supplied logical name
            Returns: the state of the file, if it exists."""
            # Right now, the filestate has "concretename", but
            # the state should really be tied to the logical, rather than
            # the concrete.  Perhaps we need a table for mapping logical
            # to concrete, or a deterministic mapping so we don't need
            # the table. 
            cur = self.connection.cursor()
            cur.execute("BEGIN;")
            cur.execute("SELECT state FROM filestate WHERE concretename=?;",
                        (logical,))
            states = cur.fetchall()
            result = None
            if states is not None and len(states) == 1:
                result = int(states[0][0])
            cur.execute("COMMIT;")
            cur.close()
            return result

        def pollFileStateByTaskId(self, taskid):
            cur = self.connection.cursor()
            cur.execute("BEGIN;")
            cur.execute("SELECT state,concretename FROM filestate WHERE taskid=?;",
                        (taskid,))
            states = cur.fetchall()
            result = states[:]
            cur.execute("COMMIT;")
            cur.close()
            return result
            result = None
            if states is not None and len(states) == 1:
                result = int(states[0][0])
            return result
            pass

        def pollFilenameById(self, id):
            cur = self.connection.cursor()
            sql = "SELECT concretename FROM filestate WHERE rowid=?;"
            cur.execute("BEGIN;")
            cur.execute(sql, (id,))
            names = cur.fetchall()
            result = None
            if names is not None and len(names) == 1:
                result = names[0][0]
            cur.execute("COMMIT;")
            cur.close()
            return result

        pass
    class SetFileStateTransaction:
        """A transaction that makes a change to a filestate"""
        def __init__(self, connection):
            self.connection = connection
            connection.isolation_level = None
            self.cursor = None
            pass
        def setByName(self, concretename, newState):
            """Sets the state of a file in the fileState table.""" 
            cur = self.connection.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute("UPDATE fileState SET state=? WHERE concretename=?;",
                        (newState,concretename))
            cur.execute("COMMIT;")
            cur.close()
            cur = None
            return True
        def setById(self, idtoken, newState):
            """Sets the state of a file in the fileState table.""" 
            cur = self.connection.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute("UPDATE fileState SET state=? WHERE rowid=?;",
                        (newState,idtoken))
            cur.execute("COMMIT;")
            cur.close()
            cur = None
            return True
        pass
    
    
    def __init__(self, dbfile=defaultDbFilename, createIfEmpty=False):
        """ctor for JobPersistence"""
        self.connected = False
        if dbfile == None: self.dbFilename = self.defaultDbFilename
        else: self.dbFilename = dbfile
        self.dbconnection = None
        self.dbcursor = None
        self.currentTaskRow = None
        self.createIfEmpty = createIfEmpty
        pass
    def connection(self):
        """only for intra-class use"""
        if not self.connected:
            # Create a connection to the database file "mydb"
            self.dbconnection = sqlite.connect(self.dbFilename)
            
            # add: cached_statements=200 param for pysqlite 2.1.0+
            # tune for faster performance
            # see: http://www.sqlite.org/pragma.html
            cur = self.dbconnection.cursor()
            while True:
                try:
                    cur.execute("PRAGMA synchronous = off;")
                    cachesize = 5000000 # 5MB?
                    cur.execute("PRAGMA cache_size = %s;" %(cachesize/1500))
                    break
                except sqlite.OperationalError, e:
                    # if db is locked, wait and retry.
                    if 'database is locked' in str(e):
                        #print >>open("/tmp/foo1","a"), os.getpid(),"dbretry"
                        time.sleep(0.5)
                        continue
                    else: #otherwise, pass the exception upwards.
                        raise e
                pass
            try:
                cur.execute("SELECT * FROM tasks WHERE 1=2")
                cur.fetchall() # flush the cursor
            except: # if the table doesn't exist, create it.
                self.buildTables(cur)
            
            cur.close()

            cur = None
            self.dbcursor = None
            self.connected = True
        return self.dbconnection
    def cursor(self):
        """only for intra-class use"""
        if self.dbcursor == None:
            # Get a Cursor object that operates in the context
            # of my Connection:
            self.dbcursor = self.connection().cursor()
        return self.dbcursor
    def commit(self):
        """force a db commit"""
        if self.connected:
            self.dbconnection.commit()
        else:
            raise "Error committing transaction: unconnected."
        

    def close(self):
        """force closing the db and releasing of db handles"""
        if self.connected:
            self.dbconnection.commit()
            if self.dbcursor is not None:
                self.dbcursor.close()
            self.dbconnection.close()
            self.connected = False
        self.dbcursor = None
        self.dbconnection = None
        # don't do this. want to close db w/o losing context.
        #self.currentTaskRow = None 
        pass
    def newPopulationTransaction(self):
        def callback(row):
            self.currentTaskRow = row
        return self.PopulationTransaction(self.connection(), callback)
    def newPreparationTransaction(self):
        assert self.currentTaskRow is not None
        return self.PreparationTransaction(self.connection(),
                                            self.currentTaskRow)
    def newFetchAndLockTransaction(self):
        assert self.currentTaskRow is not None
        return self.FetchAndLockTransaction(self.connection(),
                                            self.currentTaskRow)
    def newCommitCmdResultTransaction(self):
        return self.CommitCmdResultTransaction(self.connection())
    def newCommitAndFetchTransaction(self):
        return self.CommitAndFetchTransaction(self.connection())
    def newPollingTransaction(self):
        if self.currentTaskRow is not None:
            return self.PollingTransaction(self.connection(),
                                           self.currentTaskRow)
        else: return self.PollingTransaction(self.connection(), None)
    def newSetFileStateTransaction(self):
        return self.SetFileStateTransaction(self.connection())

    def buildTables(self, cursor=None):
        """Builds the set of tables needed for async operation"""
        ##
        ## consider having a table to store the actual script
        ## this would help for debugging and output caching
        ##
        taskcommand = ["CREATE TABLE tasks (",
                       "  taskid VARCHAR(8),",
                       "  date DATE",
                       ");"]
        cmdcommand = ["CREATE TABLE cmds (",
                      "  taskrow INTEGER(8),", 
                      "  linenum INTEGER(8),", # 8 digits of lines is a lot
                      "  cmd VARCHAR(80),", # e.g. ncwa, ncra
                      "  cmdline VARCHAR(256)", # command line(with paths)
                       "); CREATE INDEX rowlinecmd ON cmds(taskrow,linenum);"]
        inoutcommand = ["CREATE TABLE cmdFileRelation (",
                        "  taskrow INTEGER(8),",
                        "  linenum INTEGER(8),",
                        "  output TINYINT(1),",
                        "  logicalname VARCHAR(128),", # 
                        "  concretename VARCHAR(192)", #
                        ");  CREATE INDEX rowlinecmdrelation",
                        "    ON cmdFileRelation(taskrow,linenum);",
                        " CREATE INDEX namecmdrelation ",
                        " ON cmdFileRelation(concretename);"]  
        filestate = ["CREATE TABLE fileState (",
                     " taskId CHAR(8),",
                     "  concretename VARCHAR(192),",
                     "  state TINYINT(2)", #need this consistent with other table?
                     "); CREATE INDEX namestate ON fileState(concretename);",
                     " CREATE INDEX idnamestate ON fileState(taskId,concretename);"]
        readylist = ["CREATE TABLE readyList (",
                     " taskrow INTEGER(8),",
                     " linenum INTEGER(8),",
                     " concretename VARCHAR(192)", # track concrete,
                     # so we can update the filestate without querying.
                     "); CREATE INDEX rowready ON readyList(taskrow);",
                     " CREATE INDEX rowlineready",
                     " ON readyList(taskrow,linenum);"]
        useList = ["CREATE TABLE useList (",
                   " concretename VARCHAR(192),",
                   " count INTEGER(4));",
                   " CREATE INDEX nameuselist ON useList(concretename);"]

        # files can be planned, active, saved, removed, etc.
        if cursor is None:
            cur = self.cursor()
        else:
            cur = cursor
        try:
            cmd = "".join(taskcommand + ["\n"] + cmdcommand
                          + ["\n"] + inoutcommand + ["\n"] + filestate
                          + ["\n"] + readylist + ["\n"] + useList)
            cur.executescript("".join(cmd))
            print >>sys.stderr, "made tables in db (uncommit)"
        except sqlite.OperationalError:
            print >>sys.stderr, "error making tables in DB"
            pass
        pass
    
    def deleteTables(self):
        deletecmd = [ "DROP TABLE tasks;",
                      "DROP TABLE cmds;",
                      "DROP TABLE cmdFileRelation;",
                      "DROP TABLE fileState;",
                      "DROP TABLE readyList;",
                      "DROP TABLE useList;"
                    ];
        # sqlite automatically drops indexes 
        cur = self.cursor()
        try:
            cur.executescript("\n".join(deletecmd))
            print >>sys.stderr, "tables dropped ok (uncommit)"
        except sqlite.OperationalError:
            print >>sys.stderr, "error dropping tables from DB"
        pass
    
    def pollFileState(self, id):
        """Check the state of a file with the supplied id
        Returns: the state of the file, if it exists."""
        cur = self.cursor()
        cur.execute("select state from filestate where rowid=%d" % id)
        states = cur.fetchall()
        if states == []:
            return
        if len(states) == 1:
            return states[0][0]
        else:
            return
    def pollFilename(self, id):
        cur = self.cursor()
        cur.execute("select concretename from filestate where rowid=%d" % id)
        names = cur.fetchall()
        if names == []:
            return
        if len(names) == 1:
            return names[0][0]
        else:
            return
    def showState(self):
        cur = self.cursor()
        # look for tasks
        cur.execute("select taskid,rowid,date from tasks LIMIT 200;")
        ttable = cur.fetchall()
        taskdict = {}
        if ttable == []:
            print "No Tasks in DB"
        else:
            for row in ttable:
                taskid = row[0]
                if taskid not in taskdict:
                    taskdict[taskid] = row[1:]
                else:
                    print "warning, duplicate task in tasks table, id=",
                    print taskid, " Ignoring..."
            for tid in taskdict:
                entry = taskdict[tid]
                print "TaskId:", tid, "is dated",
                print entry[1]
                #self.showTaskById(tid)
                self.showTaskCommandsByRow(entry[0])
        # look for filestates
        self.showFileTable()
        self.showReadyList()
        self.showUseList()
        #print >>sys.stderr, ttable
    def showFileTable(self):
        cur = self.cursor()
        cur.execute("select rowid,concretename,state from filestate LIMIT 200")
        fstate = cur.fetchall()
        if fstate == []:
            print "No Files in DB"
        else:
            for row in fstate:
                print row[0], "Concrete", row[1], "with state", row[2],
                print "(", JobPersistence.fileStateMap[row[2]], ")"
        #print >>sys.stderr, ttable
        
    def showTaskCommandsByRow(self, taskrow):
        cur = self.cursor()
        cmd = 'select * from cmds where taskrow="%s" LIMIT 200' % (taskrow)
        cur.execute(cmd)
        for cidtuple in cur.fetchall():
            self.showCmdTuple(cidtuple)
        pass

    def showCmdTuple(self, tuple):
        """Pretty-prints a row from the cmds table"""
        cmdtemplate = "row %d, line %d, cmd %s, cmdline= %s"
        print cmdtemplate % tuple
        cur = self.cursor()
        cmd = 'select * from cmdFileRelation LEFT JOIN fileState \
        using (concretename) where taskrow=%d and linenum=%d;'
        cur.execute(cmd % (tuple[0],tuple[1]) )
        for t in cur:
            print "  ",["Inputfile","Outputfile"][t[2]],
            state = self.fileStateMap.get(t[5], "NULL")
            print "logical=%s, real=%s, state=%s (%s)" % (t[3],t[4],str(t[5]),state)
                                                          
        pass
    def showCmd(self, cid):
        cur = self.cursor()
        cmd = 'select * from cmdtable where cmdid="%s"' % (cid)
        cur.execute(cmd)
        for fields in cur:
            print '  ID %s is cmd "%s" with cmdline "%s"' % fields
        cmd = 'select * from inouttable where cmdid="%s" and output=0' % (cid)
        cur.execute(cmd)
        for fields in cur:
            print "  Inputfile: logical=%s, real=%s, state=%d" % fields[2:]
        cmd = 'select * from inouttable where cmdid="%s" and output=1' % (cid)
        cur.execute(cmd)
        for fields in cur:
            print "  Outputfile: logical=%s, real=%s, state=%d" % fields[2:]
        pass
        
    def showReadyList(self, taskrow=None):
        sql = "select * from readyList JOIN cmds USING (taskrow,linenum) LIMIT 200;"
        sqltemp = "select * from readyList JOIN cmds USING (taskrow,linenum) where taskrow=%d LIMIT 200;"
        
        cur = self.cursor()
        if taskrow is not None:
            cur.execute(sqltemp % taskrow)
        else:
            cur.execute(sql)
        for r in cur.fetchall():
            print "ready cmd: task=%d line=%d, out=%s, cmd=%s, cmdline=%s" % (
                r[0], r[1], r[2], r[3], r[4])
    def showUseList(self, taskrow=None):
        sql = "SELECT * FROM useList LIMIT 200;"
        cur = self.cursor()
        cur.execute(sql)
        for r in cur.fetchall():
            print "useList: %s count=%d\n" % r
            
    pass


def selfPopulateAndPrep(jobpers):
    inputdict = {}
    pop = jobpers.newPopulationTransaction()
    row = pop.insertTask("AABBCCDD")
    # a command with no input and one independent output (ready to go)
    pop.insertCmd(2, "ncap", "ncap -o %outf_indep.nc%")
    pop.insertInOut(2, "%outf_indep.nc%", "/tmp/temp1111outf_indep.nc", 
                    True, 1, False)
    inputdict[2] = []
    # a command with one input(orig) and one output(temp) (ready to go)
    pop.insertCmd(5, "ncwa", "ncwa in.nc %tempf_other.nc%")
    pop.insertInOut(5, "in.nc", "in.nc", 
                    False, 1, False)
    pop.insertInOut(5, "%tempf_other.nc%", "/tmp/temp0000tempf_other.nc", 
                    True, 1, True)
    inputdict[5] = []
    # a command with two inputs (orig+depend) and one output
    pop.insertCmd(10, "ncwa", "ncwa in.nc %tempf_other.nc% %outf_out.nc%")
    pop.insertInOut(10, "in.nc", "in.nc", 
                    False, 1, False)
    pop.insertInOut(10, "%tempf_other.nc%", "/tmp/temp0000tempf_other.nc", 
                    False, 1, True)
    pop.insertInOut(10, "%outf_out.nc%", "/tmp/temp1111outf_out.nc", 
                    True, 1, False)
    inputdict[10] = [("/tmp/temp0000tempf_other.nc", 1)]
    # a command with two inputs (orig+depend) and one output
    pop.insertCmd(12, "ncap", "ncwa in.nc %outf_out.nc% %outf_out2.nc%")
    pop.insertInOut(12, "in.nc", "in.nc", 
                    False, 1, False)
    pop.insertInOut(12, "%outf_out.nc%", "/tmp/temp1111outf_out.nc", 
                    False, 1, False)
    pop.insertInOut(12, "%outf_out2.nc%", "/tmp/temp2222outf_out2.nc", 
                    True, 1, False)
    inputdict[12] = [("/tmp/temp1111outf_out.nc", 1)]
    pop.finish()
    pop = None # null-out because we're paranoid
    prep = jobpers.newPreparationTransaction()
    prep.execute()
    prep = None
    return inputdict
def selfTest(args=[]):
    buildOnly = False
    if len(args) > 0:
        if "build" in args:
            buildOnly = True
    print "doing basic internal build/fill/run/delete/build/delete test."
    j = JobPersistence("sometest_db")
    j.buildTables()
    j.close()
    print " build and close"
    inputdict = selfPopulateAndPrep(j)
    #    clist = j.cmdsWithInput("/tmp/temp1111tempf_other.nc")
    j.showState()
    # now, pretend like we're executing.
    (cline, outname, linenum) = (None, None, None)
    while True:
        try:
            if cline is None:
                fetch = j.newFetchAndLockTransaction()
                (cline, outname, linenum) = fetch.execute()
                fetch = None
            else:
                print ":::opt skip fetch"
        except TypeError:
            print ":::no more lines to run!"
            break
        print ":::pretending to run %s" % (cline)
        
        #j.showState()
        print ":::fake produce %s" % (outname)
        cmtcmd = j.newCommitAndFetchTransaction()
        tup = cmtcmd.executeBlocking(outname, inputdict[linenum])
        cmtcmd = None
        (cline, outname) = (None,None)
        print "opt fetch got",tup
        if type(tup) == tuple and len(tup) == 3:
            (cline, outname, linenum) = tup
            print "opt got tuple!", tup
        #j.showState()
        
    j.showState()
    if buildOnly:
        return
    j.deleteTables()
    j.close()
    print " delete and closed"
    j.buildTables()
    j.close()
    print " build and closed"
    j.deleteTables()
    j.close()
    print " delete and closed"
    pass
def fixDbFilename(dbfilename):
    if dbfilename == []:
        return None
    elif type(dbfilename) == type([]):
        return dbfilename[0]
    else:
        return dbfilename
    
def quickShow(dbfilename = None):
    realdb = fixDbFilename(dbfilename)
    j = JobPersistence(realdb)
    print "showing tables for ssdap @ %s" % (str(realdb))
    j.showState()
    j.close()
def fileShow(dbfilename = None):
    j = JobPersistence(fixDbFilename(dbfilename))
    j.showFileTable()
    j.close()

def deleteTables(dbfilename = None):
    realdb = fixDbFilename(dbfilename)
    j = JobPersistence(realdb)
    print "ok, deleting tables from ssdap @ %s" % (str(realdb))
    j.deleteTables()
    j.close()
    pass
def buildTables(dbfilename = None):
    realdb = fixDbFilename(dbfilename)
    j = JobPersistence(realdb)
    print "ok, building new tables for ssdap @ %s" % (str(realdb))
    j.buildTables()
    j.close()
    pass
def parseArgs():
    jumptable = {"show" : quickShow,
                 "deletedb" : deleteTables,
                 "builddb" : buildTables,
                 "selftest" : selfTest
                 }
    import sys
    if (len(sys.argv) > 1) and sys.argv[1] in jumptable:
        if len(sys.argv) > 2:
            jumptable[sys.argv[1]](sys.argv[2:])
        else:
            jumptable[sys.argv[1]]()
    else:
        print sys.argv[0], "is a handy tool to manage your ",
        print "ssdap persistent state."
        print "available options:"
        for tup in jumptable:
            print tup, ",",
        print ""
        
if __name__ == '__main__':
    parseArgs()
    #test()

############################################################
# Ugly facts about ssdap_dbutil
# dap_handler_hack allows commands to have multiple outputs
# the db schema assumes that commands have only one output
# this facilitates a simpler readylist, so that we can
# update the state of the output file without doing a query
# to figure out what our output files are.
#
# however! if we assume that we have a dataflow tree/graph
# in memory, then it becomes cheap to check what we depend
# on and what depends on us.  Therefore!  We should refactor
# towards having this dep tree in memory, since it
# reduces our db load anyway.



############################################################
# spare code section
# -- delete when stable.
############################################################
exit # so the interpreter won't look at the following
#self.buildTables()
#cur.execute("select * from tasktable;")
#print >>sys.stderr, cur.fetchall()
