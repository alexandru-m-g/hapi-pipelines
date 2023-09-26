"""Functions specific to the operational presence theme."""
import hxl

from logging import getLogger
from typing import Dict

from sqlalchemy.orm import Session

from hdx.scraper.base_scraper import BaseScraper
from hapi.pipelines.database.db_operational_presence import DBOperationalPresence
from hapi.pipelines.utilities.admins import (
    Admins,
    get_admin2_to_admin1_connector_code,
)
# from hapi.pipelines.utilities.metadata import Metadata
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
        self.data = []

    def run(self):
        reader = self.get_reader()
        reader.read_hdx_metadata(self.datasetinfo)
        data = hxl.data(self.datasetinfo["url"])
        if "#org" not in data.tags or "#sector" not in data.tags or (
                "#adm1" not in data.tags and "#adm2" not in data.tags):
            logger.warning("Missing #org, #sector, or #adm1/#adm2")
        admin_info = _prescan_adms(data.columns)
        org_info = _prescan_orgs(data.columns)
        for row in data:
            pcode = _get_row_value(row, admin_info, "#adm2+code")
            level = 2
            if not pcode:
                pcode = _get_row_value(row, admin_info, "#adm1+code")
                level = 1
            if not pcode:
                # TODO: fuzzy match names to pcodes
                continue
            pcode = pcode.strip().upper()
            org_name = _get_row_value(row, org_info, "name")
            org_acronym = _get_row_value(row, org_info, "acronym")
            org_type = _get_row_value(row, org_info, "type")
            sector = _get_row_value(row, org_info, "sector")
            newrow = {
                f"#adm{level}+code": pcode,
                "#org+name": org_name,
                "#org+acronym": org_acronym,
                "#org+type": org_type,
                "#sector": sector
            }
            self.data.append(newrow)

    def populate(self):
        logger.info("Populating operational presence table")
        hapi_metadata = self.get_hapi_metadata()
        resource_ref = hapi_metadata["hdx_id"]
        reference_period_start = hapi_metadata["reference_period"]["startdate"]
        reference_period_end = hapi_metadata["reference_period"]["enddate"]
        for row in self.data:
            admin_code = row.get("#adm2+code")
            admin_level = "2"
            if not admin_code:
                admin_code = row.get("#adm1+code")
                admin_level = "1"
            org_name = row.get("#org+name")
            org_acronym = row.get("#org+acronym")
            org_type_name = row.get("#org+type")
            sector_info = row.get("#sector")
            org_type_code = self._get_org_type_code(org_type_name)
            if org_type_code == "":
                # TODO: What do we do if the org type is not in the org type table?
                logger.error(f"Org type {org_type_name} not in table")
            # TODO: find out how unique orgs are. Currently checking that combo of acronym/name/type is unique
            if (
                    org_acronym is not None
                    and org_name is not None
                    and (org_acronym, org_name, org_type_code) not in self._org.data
            ):
                self._org.populate_single(
                    acronym=org_acronym,
                    orgname=org_name,
                    org_type=org_type_code,
                    reference_period_start=reference_period_start,
                    reference_period_end=reference_period_end,
                )

            sector_name, sector_code = self._get_sector_info(sector_info)
            if sector_code == "" or sector_name == "":
                # TODO: What do we do if the sector is not in the sector table?
                logger.error(f"Sector {sector_info} not in table")

            if admin_level == "1":
                admin2_code = get_admin2_to_admin1_connector_code(
                    admin1_code=admin_code
                )
            elif admin_level == "2":
                admin2_code = admin_code
            operational_presence_row = DBOperationalPresence(
                resource_ref=resource_ref,
                org_ref=self._org.data[(org_acronym, org_name, org_type_code)],
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

    def _get_sector_info(self, sector_info: str) -> (str, str):
        # sector could be either the name or the code so have to check both
        # TODO: implement fuzzy matching of sector names/codes
        sector_data = self._sector.data
        sector_names = {name: sector_data[name] for name in sector_data}
        sector_codes = {sector_data[name]: name for name in sector_data}

        sector_code = sector_names.get(sector_info, "")
        if sector_code == "":
            sector_name = sector_codes.get(sector_code, "")
            return sector_name, sector_info
        return sector_info, sector_code


def _prescan_adms(cols):
    """ Prescan columns to figure out where we're going to pull our admin information

    The result is a dict with the keys 'adm2_pcode', 'adm2_name', 'adm1_pcode', and 'adm1_name' pointing to the
    zero-based column numbers containing the info (if present). At least one will always be present. If there is no
    column for a value, the key will not appear.
    """

    result = {}
    for i, col in enumerate(cols):
        if col.tag not in ["#adm1", "#adm2"]:
            continue
        if col.tag == "#adm2" and "code" in col.attributes:
            result["#adm2+code"] = i
        if col.tag == "#adm1" and "code" in col.attributes:
            result["#adm1+code"] = i
        if "name" in col.attributes:
            result[col.tag + "+name"] = i
    return result


def _prescan_orgs(cols):
    """ Prescan columns to figure out where we're going to pull our organisation information

    The result is a dict with the keys 'name', 'acronym', 'type', and 'sector' pointing to the zero-based
    column numbers containing the name and acronym (if present). At least one of 'name' or 'acronym' will
    always be present. If there is no column for a value (e.g. no acronym), the key will not appear.
    """

    result = {}
    for i, col in enumerate(cols):
        if col.tag == "#org" and ("acronym" not in col.attributes) and ("type" not in col.attributes):
            result["name"] = i
        if col.tag == "#org" and "acronym" in col.attributes:
            result["acronym"] = i
        if col.tag == "#org" and "type" in col.attributes:
            result["type"] = i
        if col.tag == "#sector":
            result["sector"] = i
    return result


def _get_row_value(row, info, key):
    """ Look up a value by position using key in info
    This function uses the indices produced by prescan_orgs or prescan_adms, above.
    """
    if key in info:
        try:
            return row.values[info[key]]
        except IndexError:
            pass
    return None
