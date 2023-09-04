"""OperationalPresence table."""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy import (
    DateTime,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column


class DBOperationalPresence(Base):
    __tablename__ = "operational_presence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_ref = mapped_column(Integer, nullable=False)
    org_ref = mapped_column(Integer, nullable=False)
    sector_code: Mapped[str] = mapped_column(String(32), nullable=False)
    admin2_ref: Mapped[int] = mapped_column(Integer, nullable=False)
    reference_period_start: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, index=True
    )
    reference_period_end: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, server_default=text("NULL")
    )
    source_data: Mapped[str] = mapped_column(Text)
