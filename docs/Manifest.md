# Manifest

The manifest file is used by NotBadger and BadgerDB to keep track
of the changes to the general state of the database. When a new
LSM Tree table is created a Change record is generated. It is
possible for multiple tables to be created at the same time and
changes are always wrapped in change sets. 

When the database is loaded initially, the manifest file is replayed
into memory. Each change set is just a recorded action taken by the
database. Each action is applied to the manifest in memory so the
database is aware of what partitions and tables still exist, and
what level they all are.

In BadgerDB there are no partitions, and the manifest serves only
as a method to keep track of what tables exist and what level they
are.

## Binary Format

At the beginning of the manifest file there are 8 bytes that are not
repeated at all throughout the file. The first 4 bytes is the "Magic"
text. This is used by both NotBadger and BadgerDB as a way to make
sure the file is not tampered with, and has been properly created. The
magic text is not the same between the two and NotBadger cannot read
BadgerDB manifests, and vice versa.

A version is also included in the 8 bytes. The last 4 bytes of the start
of the file is the version. This is used in both databases to indicate
the version of the manifest file. Sometimes in major updates the version
could be changed because the way the file is encoded has changed. The
version is used to tell the database if the current encoding format can
still be used to read the manifest. If the version does not match then
an error is returned when the database is opened.

Start of the manifest file:
```
+----------------------+-------------------+----------------+
| Magic Text (4 Bytes) | Version (4 Bytes) | Change Sets... |
+----------------------+-------------------+----------------+
```

Change set format:
```
+--------------------------------+---------------------------+-----------------------------+------------+
| Length Of Change Set (4 Bytes) | xxHash Checksum (4 Bytes) | Number Of Changes (4 Bytes) | Changes... |
+--------------------------------+---------------------------+-----------------------------+------------+
```

Change format:
```
+------------------------+--------------------+--------------------+----------------+------------------+-------------------------------+----------------------+
| Partition Id (4 Bytes) | Table Id (8 Bytes) | Operation (1 Byte) | Level (1 Byte) | Key Id (8 Bytes) | Encryption Algorithm (1 Byte) | Compression (1 Byte) |
+------------------------+--------------------+--------------------+----------------+------------------+-------------------------------+----------------------+
```