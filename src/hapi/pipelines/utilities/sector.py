import logging
from typing import Dict

from sqlalchemy.orm import Session

from hdx.scraper.base_scraper import BaseScraper
from hapi.pipelines.database.db_sector import DBSector

logger = logging.getLogger(__name__)


class Sector(BaseScraper):
    def __init__(self, session: Session, dataset_info: Dict[str, str]):
        self.session = session
        self.dataset_info = dataset_info
        self.data = {}

    def populate(self):
        logger.info("Populating sector table")
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        for inrow in iterator:
            code = inrow["#sector +code +v_hrinfo_sector"]
            name = inrow["#sector +name +preferred +i_en"]
            sector_row = DBSector(
                code=code,
                name=name,
            )
            self.session.add(sector_row)
            self.data[name] = code
        self._session.commit()
