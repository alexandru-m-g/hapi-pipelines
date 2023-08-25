from typing import Type

from sqlalchemy.orm import Session, declarative_base

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
]


class Tables:
    def __init__(self, session: Session):
        engine = session.get_bind()
        Base = declarative_base()
        Base.metadata.reflect(engine)

        self.create_table_classes(Base)

    @staticmethod
    def create_table_classes(Base: Type[declarative_base()]):
        for table_name in _TABLE_NAMES:
            table_class = type(
                table_name,
                (Base,),
                {"__table__": Base.metadata.tables[table_name]},
            )
            setattr(Tables, table_name, table_class)
