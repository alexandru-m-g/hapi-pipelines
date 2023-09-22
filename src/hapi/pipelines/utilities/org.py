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
        orgname,
        orgtype,
        reference_period_start,
        reference_period_end,
    ):
        logger.info(f"Adding org {orgname}")
        # TODO: add HDX link for orgs. What if there is no hdx link?
        hdx_link = ""
        org_row = DBOrg(
            hdx_link=hdx_link,
            acronym=acronym,
            name=orgname,
            org_type_code=orgtype,
            reference_period_start=reference_period_start,
            reference_period_end=reference_period_end,
        )
        self._session.add(org_row)
        self._session.commit()
        results = self._session.execute(select(DBOrg.id, DBOrg.acronym, DBOrg.name, DBOrg.org_type_code))
        for result in results:
            self.data[(result[1], result[2], result[3])] = result[0]
