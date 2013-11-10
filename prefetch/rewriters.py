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


"""
Methods turn binlog events into preheating activities
all of them can return a string query, list of strings or None
"""

def rollback(event):
   """ Default method which executes statements with a rollback at the end """
   query = ""
   if event.db:
       query += "USE %s;" % event.db

   if event.insert_id != None:
       query += "SET INSERT_ID=%d; " % event.insert_id
   if event.last_insert_id != None:
       query += "SET LAST_INSERT_ID=%d; " % event.last_insert_id

   query += "/* pos:%d */ " % event.pos
   query += event.query
   return "ROLLBACK; BEGIN; %s; ROLLBACK" % query

def fake_update(event):
    """ Execute queries, assuming that server won't do anything,
        needs fake updates support in server """
    query = ""
    if event.db:
        query += "USE %s;" % event.db

    if event.insert_id != None:
        query += "SET INSERT_ID=%d; " % event.insert_id
    if event.last_insert_id != None:
        query += "SET LAST_INSERT_ID=%d; " % event.last_insert_id

    query += "/* pos:%d */ " % event.pos
    query += event.query
    return query

