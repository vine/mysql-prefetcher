#!/usr/local/bin/python2.6 -Wignore::DeprecationWarning

import readahead
import sys
from rewriters import fake_update

""" This prefetcher will be utilizing fake changes support
within InnoDB - so it can execute statements without much
rewriting. """

prefetch = readahead.Prefetch()
prefetch.worker_init_connect = "SET SESSION "\
    "long_query_time=60, innodb_fake_changes=1, sql_log_bin=0"
prefetch.rewriter = fake_update
prefetch.window_stop = 30
prefetch.runners = 16

if __name__ == "__main__":
    prefetch.run()

