"""Dataset table."""

from hdx.database.no_timezone import Base
from sqlalchemy import Column, Integer, String


class DBDataset(Base):
    __tablename__ = "Dataset"

    id = Column(Integer, primary_key=True)
    hdx_link = Column(String(512), nullable=False)
    code = Column(String(128), nullable=False)
    title = Column(String(1024), nullable=False)
    provider_code = Column(String(128), nullable=False, index=True)
    provider_name = Column(String(512), nullable=False, index=True)
    api_link = Column(String(1024), nullable=False)
