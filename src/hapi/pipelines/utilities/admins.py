import logging
from abc import ABC
from typing import Dict, List, Literal

import hxl
from hdx.utilities.dateparse import parse_date
from hxl.filters import AbstractStreamingFilter
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_admin1 import DBAdmin1
from hapi.pipelines.database.db_admin2 import DBAdmin2
from hapi.pipelines.database.db_location import DBLocation
from hapi.pipelines.utilities.locations import Locations

logger = logging.getLogger(__name__)

_ADMIN_LEVELS = ("1", "2")


class Admins:
    def __init__(
        self,
        configuration: Dict,
        session: Session,
        locations: Locations,
        libhxl_dataset: hxl.Dataset,
    ):
        self._limit = configuration["commit_limit"]
        self._orphan_admin2s = configuration["orphan_admin2s"]
        self._session = session
        self._locations = locations
        self._libhxl_dataset = libhxl_dataset
        self.data = {}

    def populate(self):
        self._update_admin_table(
            desired_admin_level="1",
            parent_dict=self._locations.data,
        )
        self._add_admin1_connector_rows()
        results = self._session.execute(select(DBAdmin1.id, DBAdmin1.code))
        self.data = {result[1]: result[0] for result in results}
        self._update_admin_table(
            desired_admin_level="2",
            parent_dict=self.data,
        )
        self._add_admin2_connector_rows()
        results = self._session.execute(select(DBAdmin2.id, DBAdmin2.code))
        for result in results:
            self.data[result[1]] = result[0]

    def _update_admin_table(
        self,
        desired_admin_level: Literal[_ADMIN_LEVELS],
        parent_dict: Dict,
    ):
        if desired_admin_level not in _ADMIN_LEVELS:
            raise ValueError(f"Admin levels must be one of {_ADMIN_LEVELS}")
        # Filter admin level and countries
        admin_filter = _AdminFilter(
            source=self._libhxl_dataset,
            desired_admin_level=desired_admin_level,
            country_codes=list(self._locations.data.keys()),
        )
        for i, row in enumerate(admin_filter):
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
                    parent_ref = self.data[
                        get_admin1_to_location_connector_code(
                            location_code=self._orphan_admin2s[code]
                        )
                    ]
                else:
                    logger.warning(f"Missing parent {parent} for code {code}")
                    continue
            if desired_admin_level == "1":
                admin_row = DBAdmin1(
                    location_ref=parent_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
            elif desired_admin_level == "2":
                admin_row = DBAdmin2(
                    admin1_ref=parent_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
            self._session.add(admin_row)
            if i % self._limit == 0:
                self._session.commit()
        self._session.commit()

    def _add_admin1_connector_rows(self):
        for location_code, location_ref in self._locations.data.items():
            connector_code = get_admin1_to_location_connector_code(
                location_code=location_code
            )
            existing_data = (
                self._session.query(DBAdmin1)
                .filter_by(code=connector_code)
                .first()
            )
            if existing_data:
                msg = (
                    f"Admin1 table already contains {connector_code}, skipping"
                )
                logger.warning(msg)
                continue
            reference_period_start = (
                self._session.query(DBLocation)
                .filter(DBLocation.id == location_ref)
                .one()
                .reference_period_start
            )
            admin_row = DBAdmin1(
                location_ref=location_ref,
                code=get_admin1_to_location_connector_code(
                    location_code=location_code
                ),
                name="UNSPECIFIED",
                is_unspecified=True,
                reference_period_start=reference_period_start,
            )
            self._session.add(admin_row)
        self._session.commit()

    def _add_admin2_connector_rows(self):
        for admin1_code, admin1_ref in self.data.items():
            connector_code = get_admin2_to_admin1_connector_code(
                admin1_code=admin1_code
            )
            existing_data = (
                self._session.query(DBAdmin2)
                .filter_by(code=connector_code)
                .first()
            )
            if existing_data:
                msg = (
                    f"Admin2 table already contains {connector_code}, skipping"
                )
                logger.warning(msg)
                continue
            reference_period_start = (
                self._session.query(DBAdmin1)
                .filter(DBAdmin1.id == admin1_ref)
                .one()
                .reference_period_start
            )
            admin_row = DBAdmin2(
                admin1_ref=admin1_ref,
                code=connector_code,
                name="UNSPECIFIED",
                is_unspecified=True,
                reference_period_start=reference_period_start,
            )
            self._session.add(admin_row)
        self._session.commit()


def get_admin2_to_admin1_connector_code(admin1_code: str) -> str:
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


def get_admin1_to_location_connector_code(location_code: str) -> str:
    """Get the code for an unspecified admin1, based on the location code."""
    return f"{location_code}-XXX"


class _AdminFilter(AbstractStreamingFilter, ABC):
    """Filter admin rows by level and country code."""

    def __init__(
        self,
        source: hxl.Dataset,
        desired_admin_level: Literal[_ADMIN_LEVELS],
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
