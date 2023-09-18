import logging
from typing import Dict

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dateparse import parse_date
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_sector import DBSector

logger = logging.getLogger(__name__)


class Sector(BaseScraper):
    def __init__(self, session: Session, datasetinfo: Dict[str, str]):
        super().__init__(
            "sector",
            datasetinfo,
            dict(),
        )
        self._session = session
        self.data = {}

    def run(self):
        logger.info("Populating sector table")
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        for inrow in iterator:
            code = inrow["#sector +code +acronym"]
            name = inrow["#sector +name +preferred +i_en"]
            date = parse_date(inrow["#date +created"])
            sector_row = DBSector(
                code=code,
                name=name,
                reference_period_start=date,
            )
            self._session.add(sector_row)
            self.data[name] = code
        self._session.commit()
