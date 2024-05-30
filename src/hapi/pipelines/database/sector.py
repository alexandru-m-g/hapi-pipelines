"""Populate the sector table."""

import logging
from typing import Dict

from hapi_schema.db_sector import DBSector
from hdx.scraper.utilities.reader import Read
from hxl import TagPattern
from sqlalchemy.orm import Session

from ..utilities.mappings import get_code_from_name
from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Sector(BaseUploader):
    def __init__(
        self,
        session: Session,
        datasetinfo: Dict[str, str],
        sector_map: Dict[str, str],
    ):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}
        self._sector_map = sector_map
        self.pattern_to_code = {}

    def populate(self):
        logger.info("Populating sector table")

        def parse_sector_values(code: str, name: str):
            if code != "intersectoral":
                self.data[name] = code
                self.data[code] = code
                pattern = code.lower().replace("-", "_")
                pattern = TagPattern.parse(f"#*+{pattern}")
                self.pattern_to_code[pattern] = code
            sector_row = DBSector(
                code=code,
                name=name,
            )
            self._session.add(sector_row)

        reader = Read.get_reader()
        headers, iterator = reader.read(
            self._datasetinfo, file_prefix="sector"
        )
        for row in iterator:
            parse_sector_values(
                code=row["#sector +code +acronym"],
                name=row["#sector +name +preferred +i_en"],
            )

        extra_entries = {
            "Cash": "Cash programming",
            "Hum": "Humanitarian assistance (unspecified)",
            "Multi": "Multi-sector (unspecified)",
            "Intersectoral": "Intersectoral",
        }
        for code, name in extra_entries.items():
            parse_sector_values(code=code, name=name)

        self._session.commit()

    def get_sector_code(self, sector: str) -> str:
        sector_code, name_clean, add = get_code_from_name(
            name=sector,
            code_lookup=self.data,
            code_mapping=self._sector_map,
        )
        if add:
            self._sector_map[name_clean] = sector_code
        return sector_code
