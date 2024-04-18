import logging
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Literal

import hxl
from hdx.utilities.dateparse import parse_date
from hxl.filters import AbstractStreamingFilter

from .base_uploader import BaseUploader
from .locations import Locations
from hapi.pipelines.utilities.hapi_patch import HAPIPatch
from hapi.pipelines.utilities.ident_creator import generate_random_md5

logger = logging.getLogger(__name__)

_ADMIN_LEVELS = ("1", "2")
_ADMIN_LEVELS_LITERAL = Literal["1", "2"]


@dataclass
class Admin1Data:
    ident: str
    code: str
    reference_period_start: datetime


class Admins(BaseUploader):
    def __init__(
        self,
        configuration: Dict,
        libhxl_dataset: hxl.Dataset,
        locations: Locations,
        today: datetime,
    ):
        super().__init__(configuration["hapi_repo"])
        self._orphan_admin2s = configuration["orphan_admin2s"]
        self._libhxl_dataset = libhxl_dataset
        self._locations = locations
        self._today = today
        self.admin1_pcode_ident_dict = {}

    def generate_hapi_patch(self):
        logger.info("Generating admin1 patches")
        # Generate admin 1 rows patch
        rows = self._get_admin_rows(
            desired_admin_level="1",
            parent_dict=self._locations.code_location_data_dict,
        )
        with HAPIPatch(self._hapi_repo) as hapi_patch:
            patch = {
                "description": "Initial population of admin 1",
                "sequence": hapi_patch.get_sequence_number(),
                "database_schema_version": "0.7.0",
                "changes": [
                    {
                        "type": "INSERT",
                        "entity": "DBAdmin1",
                        "headers": [
                            "ident",
                            "code",
                            "name",
                            "location_ident",
                            "reference_period_start",
                            {
                                "name": "hapi_updated_date",
                                "value": self._today.isoformat(),
                            },
                            {"name": "is_unspecified", "value": "FALSE"},
                        ],
                        "values": rows,
                    }
                ],
            }
            hapi_patch.create(theme="admin1", patch=patch)
        # Generate admin 1 connector rows patch
        rows = self._get_admin1_connector_rows()
        logger.info("Generating admin2 patches")
        rows = self._get_admin_rows(
            desired_admin_level="2",
            parent_dict=self.admin1_pcode_ident_dict,
        )
        rows = self._get_admin2_connector_rows()

    def _get_admin_rows(
        self,
        desired_admin_level: _ADMIN_LEVELS_LITERAL,
        parent_dict: Dict,
    ):
        if desired_admin_level not in _ADMIN_LEVELS:
            raise ValueError(f"Admin levels must be one of {_ADMIN_LEVELS}")
        # Filter admin level and countries
        admin_filter = _AdminFilter(
            source=self._libhxl_dataset,
            desired_admin_level=desired_admin_level,
            country_codes=list(self._locations.code_location_data_dict.keys()),
        )
        rows = []
        for i, row in enumerate(admin_filter):
            ident = generate_random_md5()
            code = row.get("#adm+code")
            name = row.get("#adm+name")
            reference_period_start = parse_date(row.get("#date+start"))
            parent = row.get("#adm+code+parent")
            parent_ref = parent_dict.get(parent)
            if not parent_ref:
                if (
                    desired_admin_level == "2"
                    and code in self._orphan_admin2s.keys()
                ):
                    parent_ref = self.admin1_pcode_ident_dict[
                        _get_admin1_to_location_connector_code(
                            location_code=self._orphan_admin2s[code]
                        )
                    ]
                else:
                    logger.warning(f"Missing parent {parent} for code {code}")
                    continue
            # Columns are: ident, code, name, location_ident or admin1_ident,
            # and reference_period_start
            rows.append(
                [ident, code, name, parent_ref, reference_period_start]
            )
            if desired_admin_level == 1:
                self.admin1_pcode_ident_dict[code] = Admin1Data(
                    ident=ident,
                    code=code,
                    reference_period_start=reference_period_start,
                )
        return rows

    def _get_admin1_connector_rows(self) -> List:
        rows = []
        for (
            location_code,
            location_data,
        ) in self._locations.code_location_data_dict.items():
            ident = generate_random_md5()
            code = _get_admin1_to_location_connector_code(
                location_code=location_code
            )
            reference_period_start = location_data.reference_period_start
            # Columns are: ident, code, location_ident, reference_period_start
            rows.append(
                [ident, code, location_data.ident, reference_period_start]
            )
            self.admin1_pcode_ident_dict[code] = Admin1Data(
                ident=ident,
                code=code,
                reference_period_start=reference_period_start,
            )
        return rows

    def _get_admin2_connector_rows(self) -> List:
        rows = []
        for admin1_code, admin1_data in self.admin1_pcode_ident_dict.items():
            ident = generate_random_md5()
            code = _get_admin2_to_admin1_connector_code(
                admin1_code=admin1_code
            )
            # Columns are: ident, code, location_ident, reference_period_start
            rows.append(
                [
                    ident,
                    code,
                    admin1_data.ident,
                    admin1_data.reference_period_start,
                ]
            )
        return rows

    def get_admin_level(self, pcode: str) -> _ADMIN_LEVELS_LITERAL:
        """Given a pcode, return the admin level."""
        if pcode in self.admin1_data:
            return "1"
        elif pcode in self.admin2_data:
            return "2"
        raise ValueError(f"Pcode {pcode} not in admin1 or admin2 tables.")


def _get_admin2_to_admin1_connector_code(admin1_code: str) -> str:
    """Get the code for an unspecified admin2, based on the admin1 code.

    Note that if you need to make the connection between admin2 and
    location, and only know the location code, you'll need to pass the
    output of get_admin1_to_location_connector_code to this function, e.g.
    ```
    location_code = "ABC"
    admin1_code = get_admin1_to_location_connector_code(location_code)
    admin2_code = get_admin2_to_admin1_connector_code(admin1_code)
    ```
    """
    return f"{admin1_code}-XXX"


def _get_admin1_to_location_connector_code(location_code: str) -> str:
    """Get the code for an unspecified admin1, based on the location code."""
    return f"{location_code}-XXX"


def get_admin2_code_based_on_level(admin_code: str, admin_level: str) -> str:
    if admin_level == "national":
        admin1_code = _get_admin1_to_location_connector_code(
            location_code=admin_code
        )
        admin2_code = _get_admin2_to_admin1_connector_code(
            admin1_code=admin1_code
        )
    elif admin_level == "adminone":
        admin2_code = _get_admin2_to_admin1_connector_code(
            admin1_code=admin_code
        )
    elif admin_level == "admintwo":
        admin2_code = admin_code
    else:
        raise KeyError(
            f"Admin level {admin_level} not one of 'national',"
            f"'adminone', 'admintwo'"
        )
    return admin2_code


class _AdminFilter(AbstractStreamingFilter, ABC):
    """Filter admin rows by level and country code."""

    def __init__(
        self,
        source: hxl.Dataset,
        desired_admin_level: _ADMIN_LEVELS_LITERAL,
        country_codes: List[str],
    ):
        self._desired_admin_level = desired_admin_level
        self._country_codes = country_codes
        super(AbstractStreamingFilter, self).__init__(source)

    def filter_row(self, row):
        if (
            row.get("#geo+admin_level") != self._desired_admin_level
            or row.get("#country+code") not in self._country_codes
        ):
            return None
        return row.values
