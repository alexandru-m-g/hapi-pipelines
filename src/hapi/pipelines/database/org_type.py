import logging
from typing import Dict
from unicodedata import normalize

from hapi_schema.db_orgtype import DBOrgType
from hdx.scraper.utilities.reader import Read
from sqlalchemy.orm import Session

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
        reader = Read.get_reader()
        headers, iterator = reader.read(self._datasetinfo)
        for row in iterator:
            code = row["#org +type +code +v_hrinfo"]
            description = row["#org +type +preferred"]
            self.data[description] = code
            org_type_row = DBOrgType(
                code=code,
                description=description,
            )
            self._session.add(org_type_row)
        self._session.commit()

    def get_org_type_code(self, org_type: str) -> str:
        # TODO: implement fuzzy matching of org types (HAPI-194)
        org_type_code = self.data.get(org_type)
        if org_type_code:
            return org_type_code
        org_type = (
            normalize("NFKD", org_type)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        org_type_code = self._org_type_map.get(org_type.lower())
        return org_type_code
