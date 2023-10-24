import logging
from typing import Dict

from hapi_schema.db_org import DBOrg
from hdx.scraper.utilities.reader import Read
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Org(BaseUploader):
    def __init__(
        self,
        session: Session,
        datasetinfo: Dict[str, str],
    ):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}
        self._org_map = {}

    def populate(self):
        logger.info("Populating org mapping")
        reader = Read.get_reader()
        headers, iterator = reader.get_tabular_rows(
            self._datasetinfo["url"], headers=2, dict_form=True, format="csv"
        )
        for row in iterator:
            org_name = row.pop("#x_pattern")
            self._org_map[org_name] = row

    def populate_single(
        self,
        acronym,
        org_name,
        org_type,
        reference_period_start,
        reference_period_end,
    ):
        logger.info(f"Adding org {org_name}")
        org_row = DBOrg(
            acronym=acronym,
            name=org_name,
            org_type_code=org_type,
            reference_period_start=reference_period_start,
            reference_period_end=reference_period_end,
        )
        self._session.add(org_row)
        self._session.commit()
        results = self._session.execute(
            select(DBOrg.id, DBOrg.acronym, DBOrg.name, DBOrg.org_type_code)
        )
        for result in results:
            self.data[(result[1], result[2], result[3])] = result[0]
