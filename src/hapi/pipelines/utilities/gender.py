import logging
from typing import Dict

from hapi_schema.db_gender import DBGender
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class Gender:
    def __init__(self, session: Session, gender_descriptions: Dict[str, str]):
        self._session = session
        self._gender_descriptions = gender_descriptions
        self.data = []

    def populate(self):
        logger.info("Populating gender table")
        for gender, description in self._gender_descriptions.items():
            gender_row = DBGender(code=gender, description=description)
            self._session.add(gender_row)
            self.data.append(gender)
        self._session.commit()
