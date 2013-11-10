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

import collections
import logging
import time
import sys
import _mysql
import MySQLdb
import MySQLdb.constants.CLIENT as CL

logger = logging.getLogger(__name__)

Error = MySQLdb.Error

Config = collections.namedtuple('Config', ['host', 'port', 'username', 'password'])

class MySQL(object):
    _conn = None

    def __init__(self, config, init_connect=None):
        self.config = config
        self.init_connect = init_connect

    """ Basic MySQL connection functionality """
    def reconnect(self):
        self._conn = None

        while self._conn == None:
            try:
                self._conn = _mysql.connect(
                    user=self.config.username,
                    passwd=self.config.password,
                    port=self.config.port,
                    host=self.config.host,
                    init_command="SET SESSION wait_timeout=5",
                    client_flag=CL.MULTI_STATEMENTS | CL.MULTI_RESULTS
                )
                if self.init_connect:
                    self._conn.query(self.init_connect)
            except MemoryError:
                logger.exception("Failed to connect to %s", self.config)
                sys.exit(1)
            except MySQLdb.Error:
                logger.exception("Failed to connect to %s", self.config)
                self._conn = None
                time.sleep(1)

    def q(self, query, use_result=True):
        # Establish connection if not existing or fails to ping
        if not self._conn:
            self.reconnect()

        # We will retry just once - reconnect has infinite loop though
        for attempt in (True, False):
            try:
                self._conn.query(query)
                break  # if successful
            except MySQLdb.OperationalError:
                logger.exception("Failed to send query [%s], retrying", query)
                if attempt:
                    self.reconnect()
                    continue
                else:
                    return None

        if use_result:
            ret = []
            while True:
                r = self._conn.store_result()
                if not r:
                    # Certain statement sequences will
                    # produce multiple NULL resultsets,
                    # so we have to handle these properly
                    # ^^ - this is not a Haiku
                    if self._conn.next_result() < 0:
                        break
                    else:
                        continue

                while True:
                    row = r.fetch_row(how=1)
                    if row:
                        ret.append(row[0])
                    else:
                        break

            return ret
        else:
            return

if __name__ == "__main__":
    print MySQL(Config(sys.argv)).q("SELECT 'everything'; SET @a=1; "
                                    "SELECT 1 FROM dual WHERE NULL; SELECT 'is'; SELECT 'ok'")

