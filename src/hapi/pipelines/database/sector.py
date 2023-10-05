import logging
from typing import Dict

from hapi_schema.db_sector import DBSector
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Sector(BaseUploader):
    def __init__(self, session: Session, datasetinfo: Dict[str, str]):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}

    def populate(self):
        logger.info("Populating sector table")
        reader = Read.get_reader()
        headers, iterator = reader.read(self._datasetinfo)
        for row in iterator:
            code = row["#sector +code +acronym"]
            name = row["#sector +name +preferred +i_en"]
            date = parse_date(row["#date +created"])
            self.data[name] = code
            sector_row = DBSector(
                code=code,
                name=name,
                reference_period_start=date,
            )
            self._session.add(sector_row)
        self._session.commit()

    def get_sector_info(self, sector_info: str, info_type: str) -> (str, str):
        # TODO: implement fuzzy matching of sector names/codes
        sector_names = {name: self.data[name] for name in self.data}
        sector_codes = {self.data[name]: name for name in self.data}

        if info_type == "name":
            sector_code = sector_names.get(sector_info, "")
            return sector_code
        if info_type == "code":
            sector_name = sector_codes.get(sector_info, "")
            return sector_name
