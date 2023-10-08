"""Functions specific to the operational presence theme."""
from logging import getLogger
from typing import Dict

from hapi_schema.db_operational_presence import DBOperationalPresence
from sqlalchemy.orm import Session

from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata
from .org import Org
from .org_type import OrgType
from .sector import Sector

logger = getLogger(__name__)


class OperationalPresence(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        org: Org,
        org_type: OrgType,
        sector: Sector,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self._org = org
        self._org_type = org_type
        self._sector = sector
        self._results = results

    def populate(self):
        logger.info("Populating operational presence table")
        rows = []
        for dataset in self._results.values():
            reference_period_start = dataset["reference_period"]["startdate"]
            reference_period_end = dataset["reference_period"]["enddate"]
            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                hxl_tags = admin_results["headers"][1]
                values = admin_results["values"]

                org_name_index = hxl_tags.index("#org+name")
                org_acronym_index = hxl_tags.index("#org+acronym")
                org_type_name_index = hxl_tags.index("#org+type+name")
                try:
                    sector_name_index = hxl_tags.index("#sector+name")
                except ValueError:
                    sector_name_index = None
                try:
                    sector_code_index = hxl_tags.index("#sector+code")
                except ValueError:
                    sector_code_index = None

                for admin_code, org_names in values[org_name_index].items():
                    for i, org_name in enumerate(org_names):
                        org_acronym = values[org_acronym_index][admin_code][i]
                        org_type_name = values[org_type_name_index][
                            admin_code
                        ][i]
                        org_type_code = self._org_type.get_org_type_code(
                            org_type_name
                        )
                        if org_type_code == "":
                            # TODO: Add fuzzy matching (HAPI-194)
                            org_type_code = None
                            logger.error(
                                f"Org type {org_type_name} not in table"
                            )
                        # TODO: find out how unique orgs are. Currently checking that
                        #  combo of acronym/name/type is unique. (More clarity will come
                        #  from HAPI-166).
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

                        if sector_name_index:
                            sector_name = values[sector_name_index][
                                admin_code
                            ][i]
                            sector_code = self._sector.get_sector_info(
                                sector_name, "name"
                            )
                        else:
                            sector_code = values[sector_code_index][
                                admin_code
                            ][i]
                            sector_name = self._sector.get_sector_info(
                                sector_code, "code"
                            )
                        if sector_code == "" or sector_name == "":
                            # TODO: Fuzzy match sectors (HAPI-193)
                            logger.error(
                                f"Sector {sector_name, sector_code} not in table"
                            )

                        if admin_level == "national":
                            admin1_code = (
                                admins.get_admin1_to_location_connector_code(
                                    admin_code
                                )
                            )
                            admin2_code = (
                                admins.get_admin2_to_admin1_connector_code(
                                    admin1_code
                                )
                            )
                        if admin_level == "adminone":
                            admin2_code = (
                                admins.get_admin2_to_admin1_connector_code(
                                    admin1_code=admin_code
                                )
                            )
                        elif admin_level == "admintwo":
                            admin2_code = admin_code
                        resource_ref = self._metadata.resource_data[
                            resource_id
                        ]
                        org_ref = self._org.data[
                            (org_acronym, org_name, org_type_code)
                        ]
                        admin2_ref = self._admins.admin2_data[admin2_code]
                        row = (resource_ref, org_ref, sector_code, admin2_ref)
                        if row in rows:
                            continue
                        rows.append(row)
                        operational_presence_row = DBOperationalPresence(
                            resource_ref=resource_ref,
                            org_ref=org_ref,
                            sector_code=sector_code,
                            admin2_ref=admin2_ref,
                            reference_period_start=reference_period_start,
                            reference_period_end=reference_period_end,
                            # TODO: Add to scraper (HAPI-199)
                            source_data="not yet implemented",
                        )
                        self._session.add(operational_presence_row)
        self._session.commit()
