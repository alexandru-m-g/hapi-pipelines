import logging
from typing import Dict

from sqlalchemy.orm import Session

from hdx.scraper.base_scraper import BaseScraper
from hapi.pipelines.database.db_orgtype import DBOrgType

logger = logging.getLogger(__name__)


class OrgType(BaseScraper):
    def __init__(self, session: Session, datasetinfo: Dict[str, str]):
        super().__init__(
            "org_type",
            datasetinfo,
            dict(),
        )
        self._session = session
        self.data = {}
        self._scraped_data = []

    def run(self):
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        for row in iterator:
            code = row["#org +type +code +v_hrinfo"]
            description = row["#org +type +preferred"]
            self.data[description] = code
            self._scraped_data.append([code, description])

    def populate(self):
        logger.info("Populating org type table")
        for row in self._scraped_data:
            code = row[0]
            description = row[1]
            org_type_row = DBOrgType(
                code=code,
                description=description,
            )
            self._session.add(org_type_row)
        self._session.commit()
