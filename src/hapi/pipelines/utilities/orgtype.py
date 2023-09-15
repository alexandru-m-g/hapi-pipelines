import logging
from typing import Dict

from sqlalchemy.orm import Session

from hdx.scraper.base_scraper import BaseScraper
from hapi.pipelines.database.db_orgtype import DBOrgType

logger = logging.getLogger(__name__)


class OrgType(BaseScraper):
    def __init__(self, session: Session, dataset_info: Dict[str, str]):
        self.session = session
        self.dataset_info = dataset_info
        self.data = {}

    def populate(self):
        logger.info("Populating org type table")
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        for inrow in iterator:
            code = inrow["#org +type +code +v_hrinfo"]
            description = inrow["#org +type +preferred"]
            org_type_row = DBOrgType(
                code=code,
                description=description,
            )
            self.session.add(org_type_row)
            self.data[description] = code
        self._session.commit()
