# Directory Locks

Directory locks are used by both databases to make sure that only a single
process is trying to read/write the files for the database at a time. This
is implemented slightly differently between Unix and Windows.

## Windows
