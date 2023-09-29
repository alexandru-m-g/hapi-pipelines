import logging
from typing import Dict

from hapi_schema.db_sector import DBSector
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dateparse import parse_date
from sqlalchemy.orm import Session

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
        self._scraped_data = []

    def run(self):
        reader = self.get_reader()
        headers, iterator = reader.read(self.datasetinfo)
        for row in iterator:
            code = row["#sector +code +acronym"]
            name = row["#sector +name +preferred +i_en"]
            date = parse_date(row["#date +created"])
            self.data[name] = code
            self._scraped_data.append([code, name, date])

    def populate(self):
        logger.info("Populating sector table")

        for row in self._scraped_data:
            code = row[0]
            name = row[1]
            date = row[2]
            sector_row = DBSector(
                code=code,
                name=name,
                reference_period_start=date,
            )
            self._session.add(sector_row)
        self._session.commit()
