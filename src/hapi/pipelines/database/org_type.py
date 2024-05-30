"""Populate the org type table."""

import logging
from typing import Dict

from hapi_schema.db_org_type import DBOrgType
from hdx.scraper.utilities.reader import Read
from sqlalchemy.orm import Session

from ..utilities.mappings import get_code_from_name
from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class OrgType(BaseUploader):
    def __init__(
        self,
        session: Session,
        datasetinfo: Dict[str, str],
        org_type_map: Dict[str, str],
    ):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}
        self._org_type_map = org_type_map

    def populate(self):
        logger.info("Populating org type table")

        def parse_org_type_values(code: str, description: str) -> None:
            self.data[description] = code
            org_type_row = DBOrgType(
                code=code,
                description=description,
            )
            self._session.add(org_type_row)

        reader = Read.get_reader()
        headers, iterator = reader.read(
            self._datasetinfo, file_prefix="org_type"
        )
        for row in iterator:
            parse_org_type_values(
                code=row["#org +type +code +v_hrinfo"],
                description=row["#org +type +preferred"],
            )

        extra_entries = {
            "501": "Civil Society",
            "502": "Observer",
            "503": "Development Programme",
            "504": "Local NGO",
        }
        for code in extra_entries:
            parse_org_type_values(
                code=code,
                description=extra_entries[code],
            )

        self._session.commit()

    def get_org_type_code(self, org_type: str) -> str:
        org_type_code, name_clean, add = get_code_from_name(
            name=org_type,
            code_lookup=self.data,
            code_mapping=self._org_type_map,
        )
        if add:
            self._org_type_map[name_clean] = org_type_code
        return org_type_code
