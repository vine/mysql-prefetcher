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

import struct

UNKNOWN_EVENT = 0
START_EVENT_V3 = 1
QUERY_EVENT = 2
STOP_EVENT = 3
ROTATE_EVENT = 4
INTVAR_EVENT = 5
LOAD_EVENT = 6
SLAVE_EVENT = 7
CREATE_FILE_EVENT = 8
APPEND_BLOCK_EVENT = 9
EXEC_LOAD_EVENT = 10
DELETE_FILE_EVENT = 11
NEW_LOAD_EVENT = 12
RAND_EVENT = 13
USER_VAR_EVENT = 14
FORMAT_DESCRIPTION_EVENT = 15
XID_EVENT = 16
BEGIN_LOAD_QUERY_EVENT = 17
EXECUTE_LOAD_QUERY_EVENT = 18
TABLE_MAP_EVENT = 19
PRE_GA_WRITE_ROWS_EVENT = 20
PRE_GA_UPDATE_ROWS_EVENT = 21
PRE_GA_DELETE_ROWS_EVENT = 22
WRITE_ROWS_EVENT = 23
UPDATE_ROWS_EVENT = 24
DELETE_ROWS_EVENT = 25
INCIDENT_EVENT = 26
HEARTBEAT_LOG_EVENT = 27

class MalformedBinlogException (ValueError):
        pass

class Event(object):
    """Fixed wrapper for event data"""
    def __init__(self, pos, type, db, query, timestamp,
                 elapsed, insert_id, last_insert_id):
        self.pos = pos
        self.type = type
        self.db = db
        self.query = query
        self.timestamp = timestamp
        if elapsed < 4294967200:
            self.elapsed = elapsed
        else:
            self.elapsed = 0

        self.insert_id = insert_id
        self.last_insert_id = last_insert_id

    def __str__(self):
        db = self.db or 'None'
        return "# Binlog Event at %d DB: %s TS: %d Elapsed: %d Query:\n%s" % (
            self.pos, db, self.timestamp, self.elapsed, self.query
        )

class Binlog(object):
    """Implements methods to access binary log"""
    def __init__(self, filename):
        self.file = open(filename)

        self.filename = filename

        self.insert_id = None
        self.last_insert_id = None

        self.until = None

        self.max_event_size = 1024 * 1024

        fde = struct.unpack("<IIBIIIHH50sIB", self.file.read(4 + 76))
        (magic, timestamp, type_code, server_id, event_length,
         next_position, flags, binlog_version, server_version,
         create_timestamp, self.header_length) = fde

        if magic != 1852400382:    # 0xfe bin
            raise MalformedBinlogException("Bad magic byte")

        if type_code != FORMAT_DESCRIPTION_EVENT:
            raise MalformedBinlogException("No FDE found")
        if binlog_version != 4:
            raise NotImplementedError("Only binlog format 4 (5.x) is supported")

        tail = self.file.read(event_length - 76)
        self.header_lengths = (0, ) + struct.unpack("%dB" % len(tail), tail)
        self.position = next_position
        self.start_position = self.position

    def read_event(self):
        """Returns a dictionary with query event data
             Returns None on end-of-file conditions or False on ignored events
        """

        if self.until and self.position >= self.until:
            return None

        header_data = self.file.read(self.header_length)
        if len(header_data) < 19:
            self.file.seek(self.position)
            return None

        # Avoiding LLL here
        (timestamp, event_type,
         server_id, event_length,
         next_position, flags
        ) = struct.unpack("<IBIIIH", header_data)

        total_tail = event_length - self.header_length

        cur_position = self.position
        self.position += event_length

        # We allow very efficient skipping of large events
        if event_length > self.max_event_size:
            self.file.seek(total_tail, 1)
            return False

        event_data = self.file.read(total_tail)
        # Operating on end of file
        if len(event_data) < total_tail:
            self.file.seek(cur_position)
            return None

        if event_type == QUERY_EVENT:
            hlen = 13
            (thread_id, elapsed, db_len, error_code, status_length) = \
                struct.unpack("<IIBHH", event_data[0:hlen])
            db_offset = hlen + status_length
            db = event_data[db_offset:db_offset + db_len]
            query = event_data[db_offset + db_len + 1:]

            return Event(cur_position, 'query', db, query, timestamp,
                         elapsed, self.insert_id, self.last_insert_id)

        elif event_type in (STOP_EVENT, ROTATE_EVENT):
            return False
        elif event_type == INTVAR_EVENT:
            (intvar_type, intvar_value) = struct.unpack("<BQ", event_data)
            if intvar_type == 1:
                self.last_insert_id = intvar_value
            elif intvar_type == 2:
                self.insert_id = intvar_value
            return False
        return False

    def seek(self, pos):
        self.position = pos
        self.file.seek(pos, 0)
        self.insert_id = None
        self.last_insert_id = None

    def rewind(self):
        self.seek(self.start_position)

    def set_event_size_limit(self, limit):
        """Allows setting size limit for events to be skipped"""
        self.max_event_size = limit

    def next(self):
        """One-by-one event reader for binlog"""
        while True:
            event = self.read_event()
            if event == False:
                continue
            elif event == None:
                return
            return event

    # This is sad copy of above but for the iterator interface
    def events(self):
        """Iterator for binlog"""
        while True:
            event = self.read_event()
            if event == False:
                continue
            elif event == None:
                return
            yield event

    def __iter__(self):
        return self.events()

# Simple standalone testcase
if __name__ == "__main__":
    import sys
    bl = Binlog(sys.argv[1])
    for entry in bl:
        print entry

