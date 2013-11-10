mysql-prefetcher
================
This is a fork of [mysql@facebook's work] (http://bazaar.launchpad.net/~mysqlatfacebook/mysqlatfacebook/tools/files/head:/prefetch/), but a little bit more pythonic.

This package provides some basic MySQL Slave Prefetching tools. It can be used to speed up MySQL statement-based replication by running statements ahead of the the replication thread to prefetch data.
Rewriters are the functions responsible for transforming a replication statement to a statement you want to run.
You can provide your own rewriter or use one of the two default rewriters - the rollback rewriter and the innodb fake changes rewriter.

None of the original authors or contributors are responsible for the damage you *may* cause by using this tool.
Consider reading [this excellent post](http://dom.as/2011/12/03/replication-prefetching/) about replication delay before continuing.

rewriters
----------------
The rollback rewriter runs the statements as they come in, but issues a ROLLBACK instead of a COMMIT. While this is somewhat effective, MySQL is not well optimized for rollbacks. 
MySQL crashes have been observed from using this rewriter with a high level of concurrency.

The innodb fake changes rewriter takes advantage of a patch to MySQL. This patch is included in a couple of forks, including Facebook's and Percona's.
The idea is the same as the rollback prefetcher, except running statements don't actually cause any data changes before COMMIT. In fact, it is impossible to COMMIT with fake changes enabled.
MySQL does all the work of executing the statement, but skips the writes.

Contributing
----------------
Pull requests are welcome, but note that this is not an actively maintained project.
