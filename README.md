# duplicate_detector

I take a very liberal approach to file backup - frequently dumping
entire copies of other devices onto my desktop - which results in
many duplicate files.

This is a tool I use to
 * detect these duplicate, by comparing sha1 hashes.
 * remove redundant copies on the same disk, by creating hard links


An sqlite database is created, storing the path and hash of each file.
This is created and updated using the `hash_files.py` script.


