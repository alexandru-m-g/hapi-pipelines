"""Functions specific to the operational presence theme."""
from logging import getLogger
from typing import Dict

from hapi_schema.db_operational_presence import DBOperationalPresence
from hdx.scraper.base_scraper import BaseScraper
from sqlalchemy.orm import Session

from hapi.pipelines.utilities.admins import (
    Admins,
    get_admin1_to_location_connector_code,
    get_admin2_to_admin1_connector_code,
)
from hapi.pipelines.utilities.org import Org
from hapi.pipelines.utilities.org_type import OrgType
from hapi.pipelines.utilities.sector import Sector

logger = getLogger(__name__)


class OperationalPresence(BaseScraper):
    def __init__(
        self,
        country_code: str,
        session: Session,
        datasetinfo: Dict,
        admins: Admins,
        org: Org,
        org_type: OrgType,
        sector: Sector,
    ):
        super().__init__(
            f"operational_presence_{country_code}",
            datasetinfo,
            dict(),
        )
        self._session = session
        self._admins = admins
        self._org = org
        self._org_type = org_type
        self._sector = sector
        self._scraped_data = {}

    # TODO: make this handle all countries once metadata issue is solved
    def run(self):
        reader = self.get_reader()
        self._scraped_data = []
        _, iterator = reader.read(self.datasetinfo)
        for row in iterator:
            newrow = {}
            for input_header, output_header in zip(
                self.datasetinfo["input"],
                self.datasetinfo["output"],
            ):
                newrow[output_header] = row[input_header]
            # TODO: This is necessary because if the data is at the admin3
            #  level, there will be several duplicates. We should handle
            #  this better, e.g. define a level for each 3W file if possible.
            if newrow in self._scraped_data:
                continue
            self._scraped_data.append(newrow)

    def add_sources(self):
        return

    # TODO: make this handle all countries once metadata issue is solved
    def populate(self):
        logger.info("Populating operational presence table")
        resource_ref = self.datasetinfo["hapi_resource_metadata"]["hdx_id"]
        reference_period_start = self.datasetinfo["hapi_dataset_metadata"][
            "reference_period"
        ]["startdate"]
        reference_period_end = self.datasetinfo["hapi_dataset_metadata"][
            "reference_period"
        ]["enddate"]
        for row in self._scraped_data:
            # TODO: fuzzy match names to pcodes
            admin_code = row.get("adm2_code")
            admin_level = "admintwo"
            admin_data = self._admins.admin2_data
            if not admin_code:
                admin_code = row.get("#adm1+code")
                admin_level = "adminone"
                admin_data = self._admins.admin1_data
            admin_code = admin_code.strip().upper()
            if not admin_data[admin_code]:
                # TODO: what do we do if the pcode isn't in admins?
                logger.error(f"Admin unit {admin_code} not in admin table")
            org_name = row.get("org_name")
            org_acronym = row.get("org_acronym")
            org_type_name = row.get("org_type_name")
            org_type_code = self._get_org_type_code(org_type_name)
            if org_type_code == "":
                # TODO: What do we do if the org type is not in the org type table?
                # TODO: skip for now because too many
                pass
                # logger.error(f"Org type {org_type_name} not in table")
            # TODO: find out how unique orgs are. Currently checking that combo of acronym/name/type is unique
            if (
                org_acronym is not None
                and org_name is not None
                and (org_acronym, org_name, org_type_code)
                not in self._org.data
            ):
                self._org.populate_single(
                    acronym=org_acronym,
                    org_name=org_name,
                    org_type=org_type_code,
                    reference_period_start=reference_period_start,
                    reference_period_end=reference_period_end,
                )
            sector_name = row.get("sector_name")
            sector_code = row.get("sector_code")
            if not sector_name:
                sector_name = self._get_sector_info(sector_code, "code")
            if not sector_code:
                sector_code = self._get_sector_info(sector_name, "name")
            if sector_code == "" or sector_name == "":
                # TODO: What do we do if the sector is not in the sector table?
                # TODO: remove for now
                pass
                # logger.error(
                #    f"Sector {sector_name, sector_code} not in table"
                # )
            if admin_level == "national":
                admin1_code = get_admin1_to_location_connector_code(admin_code)
                admin2_code = get_admin2_to_admin1_connector_code(admin1_code)
            if admin_level == "adminone":
                admin2_code = get_admin2_to_admin1_connector_code(
                    admin1_code=admin_code
                )
            elif admin_level == "admintwo":
                admin2_code = admin_code
            operational_presence_row = DBOperationalPresence(
                resource_ref=resource_ref,
                org_ref=self._org.data[(org_acronym, org_name, org_type_code)],
                sector_code=sector_code,
                admin2_ref=self._admins.admin2_data[admin2_code],
                reference_period_start=reference_period_start,
                reference_period_end=reference_period_end,
                # TODO: For v2+, add to scraper
                source_data="not yet implemented",
            )
            self._session.add(operational_presence_row)
        self._session.commit()

    def _get_org_type_code(self, org_type: str) -> str:
        # TODO: implement fuzzy matching of org types
        org_type_data = self._org_type.data
        org_type_code = org_type_data.get(org_type, "")
        return org_type_code

    def _get_sector_info(self, sector_info: str, info_type: str) -> (str, str):
        # TODO: implement fuzzy matching of sector names/codes
        sector_data = self._sector.data
        sector_names = {name: sector_data[name] for name in sector_data}
        sector_codes = {sector_data[name]: name for name in sector_data}

        if info_type == "name":
            sector_code = sector_names.get(sector_info, "")
            return sector_code
        if info_type == "code":
            sector_name = sector_codes.get(sector_info, "")
            return sector_name