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


import time
import sys
import config
import _mysql
import MySQLdb
import MySQLdb.constants.CLIENT as CL

Error = MySQLdb.DatabaseError

class MySQL:
    _conn = None

    def __init__(self, init_connect=None):
        self.init_connect = init_connect

    """ Basic MySQL connection functionality """
    def reconnect(self):
        self._conn = None

        while self._conn == None:
            try:
                self._conn = _mysql.connect(
                    user=config.username,
                    passwd=config.password,
                    port=config.port,
                    host=config.host,
                    init_command="SET SESSION wait_timeout=5",
                    client_flag=CL.MULTI_STATEMENTS | CL.MULTI_RESULTS
                )
                if self.init_connect:
                    self.q(self.init_connect)
            except MemoryError:
                print sys.exc_info()
                sys.exit(1)
            except MySQLdb.Error:
                print sys.exc_info()
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
    print MySQL().q("SELECT 'everything'; SET @a=1; "
                    "SELECT 1 FROM dual WHERE NULL; SELECT 'is'; SELECT 'ok'")

