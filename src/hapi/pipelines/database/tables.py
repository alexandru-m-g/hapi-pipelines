"""Tables from DB generated using sqlacodegen."""
from sqlalchemy import (
    CHAR,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class AgeRange(Base):
    __tablename__ = "AgeRange"

    code = Column(String(32), primary_key=True)
    age_min = Column(Integer, nullable=False)
    age_max = Column(Integer)


class Dataset(Base):
    __tablename__ = "Dataset"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hdx_link = Column(String(512), nullable=False)
    code = Column(String(128), nullable=False)
    title = Column(String(1024), nullable=False)
    provider_code = Column(String(128), nullable=False, index=True)
    provider_name = Column(String(512), nullable=False, index=True)
    api_link = Column(String(1024), nullable=False)


class Gender(Base):
    __tablename__ = "Gender"

    code = Column(CHAR(1), primary_key=True)
    description = Column(String(256))


class Location(Base):
    __tablename__ = "Location"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))


class OperationalPresence(Base):
    __tablename__ = "OperationalPresence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_ref = Column(Integer, nullable=False)
    org_ref = Column(Integer, nullable=False)
    sector_code = Column(String(32), nullable=False)
    admin2_ref = Column(Integer, nullable=False)
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))
    source_data = Column(Text)


class OrgType(Base):
    __tablename__ = "OrgType"

    code = Column(String(32), primary_key=True, autoincrement=True)
    description = Column(String(512), nullable=False)


class Sector(Base):
    __tablename__ = "Sector"

    code = Column(String(32), primary_key=True)
    name = Column(String(512), nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))


class Admin1(Base):
    __tablename__ = "Admin1"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_ref = Column(ForeignKey("Location.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    Location = relationship("Location")


class Org(Base):
    __tablename__ = "Org"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hdx_link = Column(String(1024), nullable=False)
    acronym = Column(String(32), nullable=False, index=True)
    name = Column(String(512), nullable=False)
    org_type_code = Column(ForeignKey("OrgType.code"))
    reference_period_start = Column(DateTime, nullable=False, index=True)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    OrgType = relationship("OrgType")


class Population(Base):
    __tablename__ = "Population"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_ref = Column(Integer, nullable=False)
    admin2_ref = Column(Integer, nullable=False)
    gender_code = Column(ForeignKey("Gender.code"))
    age_range_code = Column(ForeignKey("AgeRange.code"))
    population = Column(Integer, nullable=False, index=True)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))
    source_data = Column(Text)

    AgeRange = relationship("AgeRange")
    Gender = relationship("Gender")


class Resource(Base):
    __tablename__ = "Resource"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_ref = Column(ForeignKey("Dataset.id"), nullable=False)
    hdx_link = Column(String(512), nullable=False)
    code = Column(String(128), nullable=False)
    filename = Column(String(256), nullable=False)
    format = Column(String(32), nullable=False)
    update_date = Column(DateTime, nullable=False, index=True)
    is_hxl = Column(Boolean, nullable=False, index=True)
    api_link = Column(String(1024), nullable=False)

    Dataset = relationship("Dataset")


class Admin2(Base):
    __tablename__ = "Admin2"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin1_ref = Column(ForeignKey("Admin1.id"))
    code = Column(String(128), nullable=False)
    name = Column(String(512), nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    is_unspecified = Column(Boolean, server_default=text("FALSE"))
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, server_default=text("NULL"))

    Admin1 = relationship("Admin1")
