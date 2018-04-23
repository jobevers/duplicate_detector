import argparse
import datetime
import logging
import os
import random
import sys

import progressbar
import sqlalchemy
from sqlalchemy import orm
import xdg.BaseDirectory

from detector import model
from detector import util


logger = logging.getLogger(__name__)


KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def main(args=None):
    parser = getParser()
    args = parser.parse_args(args)
    session = util.setupDb(args.db)
    logging.basicConfig(
        filename=args.log_file,
        level=logging.DEBUG,
        format= "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    )
    logger.info(
        "Starting script. PWD: %s, Processing: '%s'. Using: '%s'",
        os.getcwd(), args.dir, args.db
    )
    hash_files(session, args.dir, args.cutoff)


def hash_files(session, dir, cutoff):
    widgets = ['Processed: ', progressbar.Counter(), ' files (', progressbar.Timer(), ')']
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=progressbar.UnknownLength).start()
    i = 0
    for basepath, dirnames, filenames in os.walk(dir):
        # TODO: there are probably a lot of duplicate files in the
        #       hidden directories, figure out what is worth
        #       processing here and do something smarter than ignoring
        #       all of the hidden files.
        # this needs to be done in-place so that os.walk will process the changes properly
        removeItems(dirnames, isHidden)
        # shuffle dirnames so that repeated partial runs of this script
        # will process different parts.
        random.shuffle(dirnames)
        basepath = os.path.abspath(basepath)
        filenames = [f for f in filenames if not isHidden(f)]
        for filename in filenames:
            i += 1
            pbar.update(i)
            processFile(session, basepath, filename, cutoff * KB)
        session.commit()
    # progress bar seems to have a bug, so write a new line
    sys.stdout.write("\n")
    logger.info('Ending script')


def removeItems(inpt, predicate):
    copy = list(inpt)
    for name in copy:
        if predicate(name):
            inpt.remove(name)


def isHidden(filename):
    return filename.startswith('.')


def getParser(**kwds):
    parser = argparse.ArgumentParser(**kwds)
    parser.add_argument('dir', help='the directory to recursively process')
    parser.add_argument(
        '--cutoff', type=int, default=4,
        help='skip files less than this size (in kilobytes). Default: 4'
    )
    util.addDbArgument(parser)
    util.addLogArgument(parser)
    return parser


def processFile(session, basepath, filename, cutoff):
    full_path = os.path.join(basepath, filename)
    full_path = util.tryDecode(full_path)
    try:
        if not os.path.isfile(full_path):
            return
        # skip symbolic links
        if os.path.islink(full_path):
            return
        stat = os.stat(full_path)
        if stat.st_size < cutoff:
            return
        file_ = getFile(full_path, session)
        # `db_stat` is in contrast to `stat`
        # `db_stat` is the info as stored in the database (could potentially be stale)
        # `stat` is what the filesystem is reporting
        db_stat = getDbStat(stat, session)
        if not db_stat:
            if file_:
                # TODO: change the file and stat association
                raise Exception("file and stat info is out of sync")
            else:
                addFile(session, full_path)
        else:
            # see if its old, and refresh if so
            if db_stat.st_mtime < stat.st_mtime:
                logger.debug('%s: modified since last checked, rehashing', full_path)
                refreshStat(db_stat, full_path)
            # and associate it with the path
            if file_:
                assert file_.st_ino == db_stat.st_ino
            else:
                linkStat(session, full_path, db_stat)
    except OSError:
        logger.exception('Something is wrong with %s', full_path)


def getFile(full_path, session):
    """Returns true if there is an entry in the db for `full_path`"""
    try:
        return session.query(model.File).filter(model.File.filename==full_path).one()
    except orm.exc.NoResultFound:
        return None


def getDbStat(stat, session):
    """Returns true if there is an entry in the db for this inode"""
    return session.query(model.FileStat).get(stat.st_ino)


def linkStat(session, full_path, file_stat):
    """Creates a new file, from full path, and links it to the file_stat"""
    logger.debug('%s points to inode %s which is already in the db', full_path, file_stat.st_ino)
    file_ = model.File(filename=full_path)
    file_.stat = file_stat
    session.add(file_)
    return file_


def addFile(session, full_path):
    file_ = model.File(filename=full_path)
    file_.stat = makeFileStat(full_path)
    session.add(file_)
    return file_


def makeFileStat(full_path):
    stat = os.stat(full_path)
    dbstat = model.FileStat(st_ino=stat.st_ino)
    refreshStat(dbstat, full_path, stat)
    return dbstat


def refreshStat(db_stat, full_path, stat=None):
    if not stat:
        stat = os.stat(full_path)
    assert db_stat.st_ino == stat.st_ino
    hash_value = util.hashFullFile(full_path)
    logger.debug('%s hashed to %s', full_path, hash_value)
    util.copyAttributes(stat, db_stat, ('st_dev', 'st_size', 'st_nlink', 'st_mtime'))
    db_stat.full_hash_value = hash_value


if __name__ == '__main__':
    sys.exit(main())
