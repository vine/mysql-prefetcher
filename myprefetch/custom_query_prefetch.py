#!/usr/local/bin/python2.6 -Wignore::DeprecationWarning

""" Example prefetcher for a single query """

import re

delete_re = re.compile(r"DELETE FROM `(\w+)` WHERE `id` = '(\d+)' AND `type` = '(\d+)' ")
def delete_rewrite(event):
    """ Rewrite DELETE query into few SELECTs on different indexes """
    try:
        table, id_, type_ = delete_re.findall(event.query)[0]
        return (
            "SELECT 1 FROM `%s`.`%s`  WHERE id = %s " % (event.db, table, id_),
            "SELECT 1 FROM `%s`.`%s` FORCE INDEX (idtype) "\
                "WHERE id = %s and type = %s" % (event.db, table, id_, type_),
        )
    except Exception:
        return None


from myprefetch.mysql import Config
from myprefetch import readahead
import sys

prefetch = readahead.Prefetch(Config(sys.argv))
prefetch.rewriter = None
prefetch.runners = 64
prefetch.prefixes = [
    ("DELETE", delete_rewrite),
]

if __name__ == "__main__":
    prefetch.run()

