import argparse
import logging
import os
import stat
import sys

import progressbar

from detector import model
from detector import util


logger = logging.getLogger()


def main(args=None):
    args = getParser().parse_args(args)
    session = util.setupDb(args.db)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.DEBUG,
        format= "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    )

    logger.info('Starting script')

    duplicate_hashes = getDuplicateHashes(session)

    if args.limit:
        limit = min(args.limit, len(duplicate_hashes))
    else:
        limit = len(duplicate_hashes)

    pbar = progressbar.ProgressBar(
        widgets=['Working: ', progressbar.Counter(), ' ', progressbar.Bar()],
        maxval=limit
    ).start()
    pbar._time_sensitive = True

    st_start = os.statvfs('/')

    try:
        for i, hash_value in enumerate(duplicate_hashes):
            linkDuplicates(session, hash_value)
            session.commit()
            pbar.update(i)
            if i >= limit:
                break
    finally:
        # progress bar seems to have a bug, so write a new line
        sys.stdout.write("\n")
        st_end = os.statvfs('/')
        free = (st_end.f_bavail - st_start.f_bavail) * st_end.f_frsize
        # sure, this is only an approximation, but I generally am
        # not running anything else while running this script so
        # its a pretty good one
        print 'Saved {:0.1f} megabytes'.format(free / (1024.0*1024.0))
        logger.info('Ending script')


def linkDuplicates(session, hash_value):
    logger.info('Processing hash: %s', hash_value)
    files = session.query(
        model.File
    ).join(
        model.FileStat
    ).filter(
        model.FileStat.full_hash_value==hash_value
    ).all()
    files = list(removeModifiedFiles(files))

    if len(files) <= 1:
        return

    master = pickMaster(files)
    dupes = [f for f in files if f.st_ino != master.st_ino]
    for dupe in dupes:
        if os.path.islink(dupe.filename):
            continue
        linkFile(session, master, dupe)
    for f in files:
        # remove write permission so that we get an explicit error
        # if something tries to write to a file
        # since we are now sharing a lot of files, this helps
        # prevent some potentially funky problems
        removeWritePermission(f.filename)


def removeWritePermission(filename):
    # sets read-only permissions for the user and group
    current = stat.S_IMODE(os.lstat(filename).st_mode)
    without_write = current & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH
    os.chmod(filename, without_write)


def removeModifiedFiles(files):
    for f in files:
        try:
            stat = os.stat(f.filename)
        except OSError:
            logger.warning('Failed to stat %s', f.filename)
            continue
        if stat.st_mtime == stat.st_mtime:
            yield f
        else:
            logger.warning('File %s modified recently, skipping', f.filename)


def pickMaster(files):
    # grab the largest group if a group exists
    groups = util.groupBy(files, lambda f: f.st_ino)
    largest_group = max(groups.values(), key=len)
    if len(largest_group) != 1:
        files = largest_group

    files = sorted(files, key=lambda f: len(f.filename))
    for f in files:
        # prioritizing dropbox files prevents dropbox from rsyncing
        # the file and causing an unnecessary usage of bandwidth
        if f.filename.startswith('/home/jobevers/Dropbox/'):
            return f
    return files[0]


def getParser(**kwds):
    parser = argparse.ArgumentParser(**kwds)
    util.addDbArgument(parser)
    util.addLogArgument(parser)
    parser.add_argument('--limit', type=int)
    return parser


def getDuplicateHashes(session):
    query = """
        select
          a.full_hash_value
        from
          file_stat a
        inner
          join file_stat b
        on
            a.full_hash_value = b.full_hash_value
          and
            a.st_ino != b.st_ino
        group by
          a.full_hash_value
    """
    rows = session.execute(query).fetchall()
    return [r[0] for r in rows]


def isSameFileSystem(file_a, file_b):
    return util.getMount(file_a) == util.getMount(file_b)


def linkFile(session, source, link_name):
    assert source.filename != link_name.filename
    if not isSameFileSystem(source.filename, link_name.filename):
        logger.warning('Refusing to link files not on same file system')
        return
    os.rename(link_name.filename, link_name.filename + '_tmp')
    try:
        logger.debug('Linking %s to %s', link_name.filename, source.filename)
        os.link(source.filename, link_name.filename)
        stat = os.stat(link_name.filename)
        assert stat.st_ino == source.st_ino
        link_name.stat = source.stat
        os.remove(link_name.filename + '_tmp')
    except:
        os.rename(link_name.filename + '_tmp', link_name.filename)
        raise


if __name__ == '__main__':
    sys.exit(main())
