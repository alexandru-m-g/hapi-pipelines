import logging

from sqlalchemy.orm import Session

from hapi.pipelines.database.db_age_range import DBAgeRange

logger = logging.getLogger(__name__)


class AgeRange:
    def __init__(self, session: Session):
        self._session = session
        self.data = []

    def populate_single(self, age_range_code: str):
        logger.info(f"Adding age range code {age_range_code}")
        # Check and exit if already exists
        ages = age_range_code.split("-")
        if len(ages) == 2:
            # Format: 0-5
            age_min, age_max = int(ages[0]), int(ages[1])
        else:
            # Format: 80+
            age_min = int(age_range_code.replace("+", ""))
            age_max = None
        age_range_row = DBAgeRange(
            code=age_range_code, age_min=age_min, age_max=age_max
        )
        self._session.add(age_range_row)
        self._session.commit()
        self.data.append(age_range_code)
