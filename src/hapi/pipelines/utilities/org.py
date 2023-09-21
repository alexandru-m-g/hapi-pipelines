import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_org import DBOrg

logger = logging.getLogger(__name__)


class Org:
    def __init__(self, session: Session):
        self._session = session
        self.data = {}

    def populate_single(
        self,
        acronym,
        org_name,
        orgtype,
        reference_period_start,
        reference_period_end,
    ):
        logger.info(f"Adding org {org_name}")
        org_row = DBOrg(
            acronym=acronym,
            org_name=org_name,
            orgtype=orgtype,
            reference_period_start=reference_period_start,
            reference_period_end=reference_period_end,
        )
        self._session.add(org_row)
        self._session.commit()
        results = self._session.execute(select(DBOrg.id, DBOrg.acronym, DBOrg.name, DBOrg.org_type))
        for result in results:
            self.data[(result[1], result[2], result[3])] = result[0]
