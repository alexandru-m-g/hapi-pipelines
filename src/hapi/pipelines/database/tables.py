"""Reflect and return the tables from the HAPI DB."""

from typing import Type

from sqlalchemy.orm import Session, declarative_base

# Each time a new theme is added, add the tables here
_TABLE_NAMES = [
    "Admin1",
    "Admin2",
    "AgeRange",
    "Dataset",
    "Gender",
    "Location",
    "OperationalPresence",
    "Org",
    "OrgType",
    "Population",
    "Resource",
    "Sector",
]


class Tables:
    def __init__(self, session: Session):
        """
        Reflect the tables from an existing DB.

        Args:
            session ():
        """
        engine = session.get_bind()
        Base = declarative_base()
        Base.metadata.reflect(engine)

        self._create_table_classes(Base)

    @staticmethod
    def _create_table_classes(Base: Type[declarative_base()]):
        for table_name in _TABLE_NAMES:
            # Create each class dynamically with table_name as the name,
            # Base as the base, and dunder table from the Base metadata
            table_class = type(
                table_name,
                (Base,),
                {"__table__": Base.metadata.tables[table_name]},
            )
            setattr(Tables, table_name, table_class)
