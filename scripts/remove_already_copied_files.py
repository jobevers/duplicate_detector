import argparse
import logging
import os
import sys

import progressbar

from detector import hash_files
from detector import model
from detector import util


def main():
    parser = getParser()
    args = parser.parse_args()
    session = util.setupDb(args.db)
    logging.basicConfig(
        filename=args.log_file,
        level=logging.DEBUG,
        format= "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    )
    hash_files.hash_files(session, args.dirA, args.cutoff)
    hash_files.hash_files(session, args.dirB, args.cutoff)
    widgets = ['Processed: ', progressbar.Counter(), ' files (', progressbar.Timer(), ')']
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=progressbar.UnknownLength).start()
    i = 0
    for dirpath, dirnames, filenames in os.walk(args.dirA):
        for filename in filenames:
            i += 1
            pbar.update(i)
            full_path = os.path.join(dirpath, filename)
            full_path = util.tryDecode(full_path)
            db_file = hash_files.getFile(full_path, session)
            if not db_file:
                continue
            found = False
            results = session.query(
                model.FileStat
            ).filter(
                model.FileStat.full_hash_value == db_file.stat.full_hash_value
            )
            for result in results:
                for potential_match in result.hashes:
                    if potential_match.filename.startswith(args.dirB):
                        found = True
                        logger.debug('%s matches %s', full_path, potential_match.filename)
                        break
            if found:
                session.delete(db_file.stat)
                session.delete(db_file)
                session.commit()
                os.unlink(full_path)
                logger.info('rm %s', full_path)
    # progress bar seems to have a bug, so write a new line
    sys.stdout.write("\n")


def getParser(**kwds):
    parser = argparse.ArgumentParser(**kwds)
    parser.add_argument('dirA', help='the directory to delete files from')
    parser.add_argument('dirB', help='the directory to check for copies')
    parser.add_argument(
        '--cutoff', type=int, default=4,
        help='skip files less than this size (in kilobytes). Default: 4'
    )
    util.addDbArgument(parser)
    util.addLogArgument(parser)
    return parser


if __name__ == '__main__':
    logger = logging.getLogger()
    sys.exit(main())
else:
    logger = logging.getLogger(__name__)
