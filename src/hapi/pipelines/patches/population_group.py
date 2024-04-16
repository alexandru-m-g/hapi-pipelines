import logging
from typing import Dict

from hapi_schema.db_population_group import DBPopulationGroup
from hxl import TagPattern
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class PopulationGroup(BaseUploader):
    def __init__(
        self, session: Session, population_group_descriptions: Dict[str, str]
    ):
        super().__init__(session)
        self._population_group_descriptions = population_group_descriptions
        self.data = []
        self.pattern_to_code = {}

    def generate_hapi_patch(self):
        logger.info("Populating population group table")
        for (
            population_group,
            description,
        ) in self._population_group_descriptions.items():
            population_group_row = DBPopulationGroup(
                code=population_group, description=description
            )
            self._session.add(population_group_row)
            self.data.append(population_group)
            tagpattern = TagPattern.parse(f"#*+{population_group}")
            self.pattern_to_code[tagpattern] = population_group
        self._session.commit()
