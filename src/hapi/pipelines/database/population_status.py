import logging
from typing import Dict

from hapi_schema.db_population_status import DBPopulationStatus
from hxl import TagPattern
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class PopulationStatus(BaseUploader):
    def __init__(
        self, session: Session, population_status_descriptions: Dict[str, str]
    ):
        super().__init__(session)
        self._population_status_descriptions = population_status_descriptions
        self.data = []
        self.pattern_to_code = {}

    def populate(self):
        logger.info("Populating population status table")
        for (
            population_status,
            description,
        ) in self._population_status_descriptions.items():
            population_status_row = DBPopulationStatus(
                code=population_status, description=description
            )
            self._session.add(population_status_row)
            self.data.append(population_status)
            tagpattern = TagPattern.parse(f"#{population_status}")
            self.pattern_to_code[tagpattern] = population_status
        self._session.commit()
