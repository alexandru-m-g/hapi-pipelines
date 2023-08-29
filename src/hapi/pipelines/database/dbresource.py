"""Resource table."""
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from hapi.pipelines.database.base import Base


class Resource(Base):
    __tablename__ = "Resource"

    id = Column(Integer, primary_key=True)
    dataset_ref = Column(ForeignKey("Dataset.id"), nullable=False)
    hdx_link = Column(String(512), nullable=False)
    code = Column(String(128), nullable=False)
    filename = Column(String(256), nullable=False)
    format = Column(String(32), nullable=False)
    update_date = Column(DateTime, nullable=False, index=True)
    is_hxl = Column(Boolean, nullable=False, index=True)
    api_link = Column(String(1024), nullable=False)

    Dataset = relationship("Dataset")
