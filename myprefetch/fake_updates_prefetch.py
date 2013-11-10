#!/usr/local/bin/python2.6 -Wignore::DeprecationWarning

import argparse
import logging
import os

from myprefetch import readahead
from myprefetch.rewriters import fake_update
from myprefetch.mysql import Config

def main():
    logging.basicConfig()

    parser = argparse.ArgumentParser(description="""
This prefetcher will be utilizing fake changes support within InnoDB -
so it can execute statements without much rewriting.""".strip(),
                                     fromfile_prefix_chars='@',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--host', default='localhost', help='MySQL server hostname')
    parser.add_argument('--port', default=3306, type=int, help='MySQL server port')
    parser.add_argument('--username', '-u', default='root', help='MySQL username')
    parser.add_argument('--password', '-p', default='', help='MySQL password')
    parser.add_argument('--runners', default=4, type=int,
                        help='Number of statement runner threads to use')
    parser.add_argument('--threshold', default=1.0, type=float,
                        help='Minimum "seconds behind master" before we start prefetching')
    parser.add_argument('--window_start', default=13, type=int,
                        help='How far into the future from seconds behind master to start '
                             'prefetching.')
    parser.add_argument('--window_stop', default=30, type=int,
                        help='How far into the future to prefetch.')
    parser.add_argument('--elapsed_limit', default=4, type=int,
                        help='We won\'t try to prefetch statements that took longer than '
                             '--elapsed_limit on the master')
    parser.add_argument('--logpath', default="/var/lib/mysql",
                        help='How far into the future to prefetch.')
    args = vars(parser.parse_args())

    if not os.path.isdir(args['logpath']):
        raise Exception("%s is not a valid directory" % (args['logpath'],))

    config = Config(**{k: args.pop(k) for k in ('host', 'port', 'username', 'password')})

    prefetch = readahead.Prefetch(config, **args)
    prefetch.worker_init_connect = "SET SESSION "\
        "long_query_time=60, innodb_fake_changes=1, sql_log_bin=0"
    prefetch.rewriter = fake_update
    prefetch.run()

if __name__ == "__main__":
    main()

