import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import declarative


Base = declarative.declarative_base()


class File(Base):
    __tablename__ = 'file'
    id_ = sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    filename = sqlalchemy.Column(sqlalchemy.String, index=True)
    st_ino = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('file_stat.st_ino'))
    stat = orm.relationship('FileStat', back_populates="hashes")
    sent_file = orm.relationship('FilesSentToACD', uselist=False, backref=orm.backref('file'))


class FileStat(Base):
    __tablename__ = 'file_stat'
    st_ino = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    st_dev = sqlalchemy.Column(sqlalchemy.String)
    st_size = sqlalchemy.Column(sqlalchemy.Integer)
    st_nlink = sqlalchemy.Column(sqlalchemy.Integer)
    st_mtime = sqlalchemy.Column(sqlalchemy.Float)
    full_hash_value = sqlalchemy.Column(sqlalchemy.String, index=True)
    hashes = orm.relationship('File', back_populates='stat')


# this is not implemented.
class FilesSentToACD(Base):
    __tablename__ = 'files_sent_to_acd'
    file_id = sqlalchemy.Column(
        'id', sqlalchemy.Integer, sqlalchemy.ForeignKey('file.id'), primary_key=True)
    acd_filename = sqlalchemy.Column(sqlalchemy.String)
    sent_time = sqlalchemy.Column(sqlalchemy.DateTime)
