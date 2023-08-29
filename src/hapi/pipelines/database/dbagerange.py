"""Resource table."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AgeRange(Base):
    __tablename__ = "AgeRange"

    code = Column(String(32), primary_key=True)
    age_min = Column(Integer, nullable=False)
    age_max = Column(Integer)
