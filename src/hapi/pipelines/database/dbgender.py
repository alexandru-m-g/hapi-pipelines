"""Gender table."""

from sqlalchemy import CHAR, Column, String

from hapi.pipelines.database.base import Base


class DBGender(Base):
    __tablename__ = "Gender"

    code = Column(CHAR(1), primary_key=True)
    description = Column(String(256))
