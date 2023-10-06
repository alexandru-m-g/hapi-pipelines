import logging

from hapi_schema.db_org import DBOrg
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Org(BaseUploader):
    def __init__(self, session: Session):
        super().__init__(session)
        self.data = {}

    def populate(self):
        raise NotImplementedError()

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
            # TODO: Populate hdx_link (HAPI-211)
            hdx_link="not yet implemented",
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
