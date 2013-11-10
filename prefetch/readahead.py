#!/usr/bin/python
#
#   Copyright 2011 Facebook
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import sys
import os
import mysql
import traceback
import time

from threading import Thread
import Queue
from binlog import Binlog
import rewriters
import re

# Return whole string after multiple comment groups
initial_comment_re = re.compile("^\s*(/\*.*?\*/\s*)*(.*)")
def strip_initial_comment(query):
    return initial_comment_re.findall(query)[-1][-1]


# Debugging output
def d(s):
    # print "DEBUG:", s
    pass


class Executor (object):
    """Rewriters inheriting from Executor are given more freedom to execute"""
    def run(self, event, db):
        """Must implement run() to do anything, or it is noop"""
        pass


class Slave (mysql.MySQL):
    def slave_status(self):
        status = self.q("SHOW SLAVE STATUS")
        return status and status[0] or None

    def sleep(self, threshold, comment=None):
        if comment:
            self.q("/* %s */ SELECT SLEEP(%f)" % (comment, threshold))
        else:
            self.q("SELECT SLEEP(%f)" % threshold)


class Runner(Thread):
    """Worker thread that runs events placed on a queue"""
    def __init__(self, prefetcher):
        self.db = None
        self.queue = prefetcher.queue
        self.detect = prefetcher.detect
        Thread.__init__(self)
        self.daemon = True

    def run(self):
        try:
            if not self.db:
                self.db = Slave()

            while True:
                event = self.queue.get(block=True)
                rewriter = self.detect(event)
                if rewriter == None:
                    continue

                try:
                    # We give up full control to Executors
                    if isinstance(rewriter, Executor):
                        rewriter.run(event, self.db)
                        continue

                    queries = rewriter(event)
                    if queries == None:
                        continue

                    if type(queries) == str:
                        queries = (queries, )
                    for query in queries:
                        self.db.q("/* prefetching at %d */ %s" %
                                    (event.pos, query))

                except mysql.Error:
                    self.db.q("ROLLBACK")
        except:
            traceback.print_exc()
            os.kill(os.getpid(), 9)


class Prefetch:
    """Main prefetching chassis"""
    def __init__(self):
        # Number of runner threads
        self.runners = 4
        # How much do we lag before we step in
        self.threshold = 1.0
        # how much do we jump to future from actual execution (in seconds)
        self.window_start = 1
        # how much ahead we actually work (in seconds)
        self.window_stop = 240
        # Time limit (seconds) - based on elapsed time on master,
        # should we try prefetching
        self.elapsed_limit = 4
        # Where are all the logs
        self.logpath = "/var/lib/mysql/"
        # How often should checks run (hz)
        self.frequency = 10
        # Should comments be stripped from query inside event
        self.strip_comments = False
        # Custom rewriters for specific queries
        self.prefixes = [
          # ("INSERT INTO customtable", rewriters.custom_table_rewriter),
        ]
        self.rewriter = rewriters.rollback
        self.wait_for_replication = True
        self.worker_init_connect = "SET SESSION long_query_time=60"

        # Better not to override this from outside
        self.queue = None

    def detect(self, event):
        """Return rewriting method for event"""
        if event.query in (None, "", "BEGIN", "COMMIT", "ROLLBACK"):
            return None

        # Allow custom per-prefix rewriter
        if self.prefixes:
            query = strip_initial_comment(event.query)
            if self.strip_comments:
                event.query = query
            for prefix, rewriter in self.prefixes:
                if isinstance(prefix, str):
                    if query.startswith(prefix):
                        return rewriter
                elif prefix.match(query):
                    return rewriter

        return self.rewriter

    def binlog_from_status(self, status):
        """ Open binlog object based on SHOW SLAVE STATUS """
        filepath = self.logpath + status["Relay_Log_File"]
        pos = int(status["Relay_Log_Pos"])
        binlog = Binlog(filepath)
        binlog.seek(pos)
        return binlog

    def prefetch(self):
        """Main service routine to glue everything together"""
        slave = Slave(init_connect=self.worker_init_connect)
        prefetched = None
        cycles_count = 0

        self.queue = Queue.Queue(self.runners * 4)
        for thread in range(self.runners):
            Runner(self).start()

        while True:
            d("Running prefetch check")

            st = slave.slave_status()
            if not st or st['Slave_SQL_Running'] != "Yes":
                if not self.wait_for_replication:
                    raise EnvironmentError("Replication not running! Bye")
                else:
                    time.sleep(10)
                    continue

            if st['Seconds_Behind_Master'] is not None:
                # We compensate for negative lag here
                lag = max(int(st['Seconds_Behind_Master']), 0)

                if lag <= self.threshold:
                    d("Skipping for now, lag is below threshold")
                    slave.sleep(1.0 / self.frequency,
                                "Lag (%d) is below threshold (%d)" % \
                                (lag, self.threshold))
                    continue

            binlog = self.binlog_from_status(st)
            # Look at where we are
            event = binlog.next()

            # Though this should not happen usually...
            if not event:
                slave.sleep(1.0 / self.frequency, "Reached the end of binlog")
                continue

            sql_time = event.timestamp

            # Jump ahead if we have already prefetched on this file
            if prefetched and prefetched['file'] == binlog.filename and \
                    prefetched['pos'] > event.pos:

                d("Jump to %d" % prefetched['pos'])
                binlog.seek(prefetched['pos'])

            # Iterate through the stuff in front
            for event in binlog:
                if len(event.query) < 10:
                    continue
                # Skip few entries, leave them for SQL thread
                if event.timestamp < sql_time + self.window_start:
                    d("Skipping, too close to SQL thread")
                    continue

                if event.timestamp > sql_time + self.window_stop:
                    d("Breaking, too far from SQL thread")
                    break

                if event.elapsed > self.elapsed_limit:
                    d("Skipping, elapsed too long")
                    continue

                try:
                    self.queue.put(event, block=True, timeout=1)
                except Queue.Full:
                    d("Queue full, breaking out of binlog")
                    break
                cycles_count += 1
                if not cycles_count % 10000:
                    break

            d("Got ahead to %d" % binlog.position)
            prefetched = {'pos': binlog.position, 'file': binlog.filename}
            slave.sleep(1.0 / self.frequency,
                "Got ahead to %d" % binlog.position)

    def run(self):
        try:
            self.prefetch()
        except:
            traceback.print_exc()
            sys.exit()

if __name__ == "__main__":
    """As standalone application it will start rollback-based prefetcher"""
    Prefetch().run()

