#!/usr/local/bin/python2.6 -Wignore::DeprecationWarning

""" Example prefetcher for a single query """

import re

delete_re = re.compile("DELETE FROM `(\w+)` WHERE `id` = '(\d+)' AND `type` = '(\d+)' ")
def delete_rewrite(event):
    """ Rewrite DELETE query into few SELECTs on different indexes """
    try:
        table, id, type = delete_re.findall(event.query)[0]
        return (
            "SELECT 1 FROM `%s`.`%s`  WHERE id = %s " % (event.db, table, id),
            "SELECT 1 FROM `%s`.`%s` FORCE INDEX (idtype) "\
                "WHERE id = %s and type = %s" % (event.db, table, id, type),
        )
    except:
        return None


import readahead
import sys

prefetch = readahead.Prefetch()
prefetch.rewriter = None
prefetch.runners = 64
prefetch.prefixes = [
    ("DELETE", delete_rewrite),
]

if __name__ == "__main__":
    prefetch.run()

