"""Gender table."""

from hdx.database.no_timezone import Base
from sqlalchemy import CHAR, Column, String


class DBGender(Base):
    __tablename__ = "Gender"

    code = Column(CHAR(1), primary_key=True)
    description = Column(String(256))
