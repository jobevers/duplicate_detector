import argparse
import logging
import os
import stat
import sys

from detector import model
from detector import util


logger = logging.getLogger()


def main(args=None):
    parser = argparse.ArgumentParser()
    util.addDbArgument(parser)
    args = parser.parse_args(args)
    session = util.setupDb(args.db)

    logging.basicConfig(
        level=logging.DEBUG,
        format= "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    )

    for file_obj in session.query(model.File):
        if not os.path.exists(file_obj.filename):
            logger.info('Removing %s from db', file_obj.filename)
            session.delete(file_obj)
    session.commit()
    session.execute("""
      delete from file_stat
      where st_ino in (
        select fs.st_ino
        from file_stat fs
        left outer join file f
        on fs.st_ino = f.st_ino
        where f.filename is null
    )
    """)


if __name__ == '__main__':
    sys.exit(main())
