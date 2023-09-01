"""Population table."""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hapi.pipelines.database.dbagerange import DBAgeRange  # noqa: F401
from hapi.pipelines.database.dbgender import DBGender  # noqa: F401


class DBPopulation(Base):
    __tablename__ = "population"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_ref: Mapped[int] = mapped_column(Integer, nullable=False)
    admin2_ref: Mapped[int] = mapped_column(Integer, nullable=False)
    gender_code: Mapped[str] = mapped_column(ForeignKey("gender.code"))
    age_range_code: Mapped[str] = mapped_column(ForeignKey("age_range.code"))
    population: Mapped[int] = mapped_column(
        Integer, nullable=False, index=True
    )
    reference_period_start: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )
    reference_period_end: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, server_default=text("NULL")
    )
    source_data: Mapped[str] = mapped_column(Text)

    age_range = relationship("DBAgeRange")
    gender = relationship("DBGender")
