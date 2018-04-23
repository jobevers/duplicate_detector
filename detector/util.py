import __main__
import collections
import hashlib
import logging
import os
import string

import sqlalchemy
from sqlalchemy import orm
import xdg.BaseDirectory

from detector import model


logger = logging.getLogger(__name__)


def addDbArgument(parser):
    data_dir = xdg.BaseDirectory.save_data_path('duplicate_detector')
    default_db = os.path.join(data_dir, 'files.db')
    parser.add_argument(
        '--db',
        default=default_db,
        help='the sqlite database storing hash information. Default: {}'.format(default_db)
    )


def addLogArgument(parser):
    data_dir = xdg.BaseDirectory.save_data_path('duplicate_detector')
    default_log = os.path.join(data_dir, defaultName('detector') + '.log')
    parser.add_argument(
        '--log-file',
        default=default_log,
        help="File to log to. Default: {}".format(default_log)
    )


def setupDb(db_filename):
    engine = sqlalchemy.create_engine('sqlite:///{}'.format(db_filename))
    model.Base.metadata.create_all(engine)
    session = orm.sessionmaker(bind=engine)()
    return session


def defaultName(fallback):
    """Attempts to return a reasonable name for the running program"""
    try:
        name = os.path.basename(__main__.__file__)
    except AttributeError:
        try:
            name = sys.argv[0]
        except IndexError:
            name = fallback
    return name


def hashFile(filename, sample_size=2<<17, sample_points=4):
    """Return the sha1 of the sampled contents of a file.

    If the total filesize is less than sample_size * sample_points
    then the entire file is read and hashed.

    filename: name of file to read
    sample_size: for each sample, how much of the file to read
    sample_points: number of samples to take
    """
    logger.debug("Hashing %s", filename)
    file_size = os.path.getsize(filename)
    h = hashlib.sha1()
    with open(filename) as f:
        if file_size < (sample_size * sample_points):
            h.update(f.read())
        else:
            for i in range(0, sample_points):
                seek = i * file_size / sample_points
                f.seek(seek)
                h.update(f.read(sample_size))
    hash = h.hexdigest()
    return hash


def groupBy(iterable, key):
    groups = collections.defaultdict(list)
    for item in iterable:
        groups[key(item)].append(item)
    return groups


def hashFullFile(filename):
    h = hashlib.sha1()
    with open(filename) as f:
        h.update(f.read())
    return h.hexdigest()


def setStat(obj, stat):
    attrs = ('st_ino', 'st_dev', 'st_size', 'st_nlink', 'st_mtime')
    copyAttributes(stat, obj, attrs)


def copyAttributes(src, dst, attrs):
    for attr in attrs:
        setattr(dst, attr, getattr(src, attr))


def getMount(path):
    """returns the mount point containing the input path"""
    while path != os.path.sep:
        if os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return os.path.sep


def tryDecode(string):
    codec = ('ascii', 'latin-1', 'utf-8')
    for c in codec:
        try:
            return string.decode(c)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(string)

