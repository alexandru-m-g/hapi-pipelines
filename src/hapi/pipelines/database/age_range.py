import logging
from typing import List

from hapi_schema.db_age_range import DBAgeRange
from hxl import TagPattern
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class AgeRange(BaseUploader):
    def __init__(self, session: Session, age_range_codes: List[str]):
        super().__init__(session)
        self.data = age_range_codes
        self.pattern_to_code = {}

    def populate(self):
        logger.info("Populating age ranges table")
        for age_range_code in self.data:
            self.populate_single(age_range_code)
        self._session.commit()

    def populate_single(self, age_range_code: str):
        ages = age_range_code.split("-")
        if len(ages) == 2:
            # Format: 0-5
            age_min, age_max = int(ages[0]), int(ages[1])
            pattern = f"age{age_min}_{age_max}"
        else:
            # Format: 80+
            age_min = int(age_range_code.replace("+", ""))
            age_max = None
            pattern = f"age{age_min}plus"
        age_range_row = DBAgeRange(
            code=age_range_code, age_min=age_min, age_max=age_max
        )
        self._session.add(age_range_row)
        tagpattern = TagPattern.parse(f"#*+{pattern}")
        self.pattern_to_code[tagpattern] = age_range_code
