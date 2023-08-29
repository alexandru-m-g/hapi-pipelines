"""Gender table."""
from sqlalchemy import CHAR, Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Gender(Base):
    __tablename__ = "Gender"

    code = Column(CHAR(1), primary_key=True)
    description = Column(String(256))
