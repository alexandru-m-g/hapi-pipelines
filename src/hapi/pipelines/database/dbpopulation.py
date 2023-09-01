"""Population table."""

from hdx.database.no_timezone import Base
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    text,
)
from sqlalchemy.orm import relationship

from hapi.pipelines.database.dbagerange import DBAgeRange  # noqa
from hapi.pipelines.database.dbgender import DBGender  # noqa


class DBPopulation(Base):
    __tablename__ = "population"

    id = Column(Integer, primary_key=True)
    resource_ref = Column(Integer, nullable=False)
    admin2_ref = Column(Integer, nullable=False)
    gender_code = Column(ForeignKey("gender.code"))
    age_range_code = Column(ForeignKey("age_range.code"))
    population = Column(Integer, nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))
    source_data = Column(Text)

    age_range = relationship("DBAgeRange")
    gender = relationship("DBGender")
