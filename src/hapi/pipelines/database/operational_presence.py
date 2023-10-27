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
        number_duplicates = 0
        for dataset in self._results.values():
            reference_period_start = dataset["reference_period"]["startdate"]
            reference_period_end = dataset["reference_period"]["enddate"]
            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                hxl_tags = admin_results["headers"][1]
                values = admin_results["values"]

                org_name_index = hxl_tags.index("#org+name")
                org_acronym_index = hxl_tags.index("#org+acronym")
                try:
                    org_type_name_index = hxl_tags.index("#org+type+name")
                except ValueError:
                    org_type_name_index = None
                sector_index = hxl_tags.index("#sector")

                for admin_code, org_names in values[org_name_index].items():
                    for i, org_name in enumerate(org_names):
                        # TODO: find the country code for get_org_info parameter "location"
                        org_info = self._org.get_org_info(
                            org_name, location="Country code"
                        )
                        org_name = org_info.get("#org+name")
                        org_acronym = org_info.get(
                            "#org+acronym",
                            values[org_acronym_index][admin_code][i],
                        )
                        org_type_code = org_info.get("#org+type+code")
                        if not org_type_code:
                            if org_type_name_index:
                                org_type_name = values[org_type_name_index][
                                    admin_code
                                ][i]
                                org_type_code = (
                                    self._org_type.get_org_type_code(
                                        org_type_name
                                    )
                                )
                        if not org_type_code:
                            logger.error("Org type missing or not in table")
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
                        sector = values[sector_index][admin_code][i]
                        sector_code = self._sector.get_sector_code(sector)
                        if not sector_code:
                            logger.error(f"Sector {sector} not in table")

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
                            number_duplicates += 1
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
        logger.info(
            f"There were {number_duplicates} duplicate operational presence rows!"
        )
