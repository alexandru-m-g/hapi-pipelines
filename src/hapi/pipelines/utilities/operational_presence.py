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
        session: Session,
        datasetinfo: Dict,
        admins: Admins,
        org: Org,
        org_type: OrgType,
        sector: Sector,
    ):
        super().__init__(
            "operational_presence",
            datasetinfo,
            dict(),
        )
        self._session = session
        self._admins = admins
        self._org = org
        self._org_type = org_type
        self._sector = sector
        self._scraped_data = {}

    def run(self):
        reader = self.get_reader()
        for country in self.datasetinfo:
            self._scraped_data[country] = []
            _, iterator = reader.read(self.datasetinfo[country])
            for row in iterator:
                newrow = {}
                for input_header, output_header in zip(
                    self.datasetinfo[country]["input"],
                    self.datasetinfo[country]["output"],
                ):
                    newrow[output_header] = row[input_header]
                if newrow in self._scraped_data[country]:
                    continue
                self._scraped_data[country].append(newrow)

    def add_sources(self):
        return

    def populate(self):
        logger.info("Populating operational presence table")
        for country in self._scraped_data:
            hapi_metadata = self.datasetinfo[country].get("hapi_metadata")
            resource_ref = hapi_metadata["hdx_id"]
            reference_period_start = hapi_metadata["reference_period"][
                "startdate"
            ]
            reference_period_end = hapi_metadata["reference_period"]["enddate"]
            for row in self._scraped_data[country]:
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
                    logger.error(f"Org type {org_type_name} not in table")
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
                    logger.error(
                        f"Sector {sector_name, sector_code} not in table"
                    )
                if admin_level == "national":
                    admin1_code = get_admin1_to_location_connector_code(
                        admin_code
                    )
                    admin2_code = get_admin2_to_admin1_connector_code(
                        admin1_code
                    )
                if admin_level == "adminone":
                    admin2_code = get_admin2_to_admin1_connector_code(
                        admin1_code=admin_code
                    )
                elif admin_level == "admintwo":
                    admin2_code = admin_code
                operational_presence_row = DBOperationalPresence(
                    resource_ref=resource_ref,
                    org_ref=self._org.data[
                        (org_acronym, org_name, org_type_code)
                    ],
                    sector_code=sector_code,
                    admin2_ref=self._admins.data[admin2_code],
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
