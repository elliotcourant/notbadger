# NotBadger

NotBadger is a project to fork BadgerDB. The primary goal of NotBadger is to create a database that
supports multiple LSM Trees as a single database. The trees would be stored separately on the disk
as well as read and written to separately. The trees are entirely separate in every way except that
writes to the trees are all written to the same write ahead log and manifest file. 
This separation is referred to as partitions in this code. A single partition represents what would
normally be a single Badger database. But transactions should be able to write atomically to
multiple partitions.

Most of the code in NotBadger is an exact copy of BadgerDB with only minor changes.

NotBadger also aims to provide detailed and in-depth documentation about the inner workings of both
NotBadger and BadgerDB.