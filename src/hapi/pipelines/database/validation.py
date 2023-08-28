"""Reflect and return the tables from the HAPI DB."""
from logging import getLogger

from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from hapi.pipelines.database.tables import metadata

logger = getLogger(__name__)


def validate(session: Session):
    logger.info("Validating schema against table")

    engine = session.get_bind()
    reflected_metadata = MetaData()
    reflected_metadata.reflect(bind=engine)

    # Iterate through the tables in the reflected metadata
    for reflected_table in reflected_metadata.tables.values():
        # Try to get the corresponding defined table from tables.py
        defined_table = metadata.tables.get(reflected_table.name)

        # Compare the reflected table with the defined table
        if defined_table is None:
            raise RuntimeError(
                f"The table {reflected_table.name} is missing"
                f"from this schema"
            )
        # Compare columns, keys, constraints, etc.
        if set(reflected_table.columns.keys()) != set(
            defined_table.columns.keys()
        ):
            msg = (
                f"Columns mismatch for table {reflected_table.name}: "
                f"Reflected: {reflected_table.columns.keys()}, "
                f"Defined: {defined_table.columns.keys()}"
            )
            raise RuntimeError(msg)
