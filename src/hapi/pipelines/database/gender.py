import logging
from typing import Dict

from hapi_schema.db_gender import DBGender
from hxl import TagPattern
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Gender(BaseUploader):
    def __init__(self, session: Session, gender_descriptions: Dict[str, str]):
        super().__init__(session)
        self._gender_descriptions = gender_descriptions
        self.data = []
        self.pattern_to_code = {}

    def populate(self):
        logger.info("Populating gender table")
        for gender, description in self._gender_descriptions.items():
            gender_row = DBGender(code=gender, description=description)
            self._session.add(gender_row)
            self.data.append(gender)
            tagpattern = TagPattern.parse(f"#*+{gender}")
            self.pattern_to_code[tagpattern] = gender
        self._session.commit()
