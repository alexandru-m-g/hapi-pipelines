import logging
from typing import Dict

from hapi_schema.db_org_type import DBOrgType
from hdx.scraper.utilities.reader import Read
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class OrgType(BaseUploader):
    def __init__(self, session: Session, datasetinfo: Dict[str, str]):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}

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
        org_type_code = self.data.get(org_type, "")
        return org_type_code
