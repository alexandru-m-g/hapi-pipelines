import logging
from abc import ABC
from datetime import datetime
from typing import Dict, List

import hxl
from hdx.utilities.dateparse import parse_date
from hxl import Dataset, InputOptions
from hxl.filters import AbstractStreamingFilter
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_admin1 import DBAdmin1
from hapi.pipelines.database.db_admin2 import DBAdmin2
from hapi.pipelines.utilities.locations import Locations

logger = logging.getLogger(__name__)


class Admins:
    def __init__(
        self, configuration: Dict, session: Session, locations: Locations
    ):
        self.limit = configuration["commit_limit"]
        self.session = session
        self.locations = locations
        self.data = {}

    def populate(self):
        try:
            admin_info = hxl.data(
                "https://data.humdata.org/dataset/cb963915-d7d1-4ffa-90dc-31277e24406f/resource/f65bc260-4d8b-416f-ac07-f2433b4d5142/download/global_pcodes_adm_1_2.csv",
                InputOptions(encoding="utf-8"),
            ).cache()
        except OSError:
            logger.exception("Download of admin info from dataset failed!")
            return

        self._update_admin_table(desired_adminlevel="1", admin_info=admin_info)
        results = self.session.execute(select(DBAdmin1.id, DBAdmin1.code))
        self.data = {result[1]: result[0] for result in results}
        self._update_admin_table(desired_adminlevel="2", admin_info=admin_info)
        results = self.session.execute(select(DBAdmin2.id, DBAdmin2.code))
        for result in results:
            self.data[result[1]] = result[0]

    def _update_admin_table(
        self, desired_adminlevel: str, admin_info: Dataset
    ):
        parent_dict = {"1": self.locations.data, "2": self.data}
        admin_filter = AdminFilter(
            source=admin_info,
            desired_admin_level=desired_adminlevel,
            country_codes=list(self.locations.data.keys()),
        )
        for i, row in enumerate(admin_filter):
            # Get the info needed
            code = row.get("#adm+code")
            name = row.get("#adm+name")
            reference_period_start = parse_date(row.get("#date+start"))
            parent = row.get("#adm+code+parent")
            parent_ref = parent_dict[desired_adminlevel].get(parent)
            # Catch edge cases
            if not parent_ref:
                # TODO: should this be in the config somehow?
                # Abyei
                if code == "SS0001":
                    # TODO: change this to blank ref
                    logger.warning(f"Changing {code}")
                    parent_ref = self.data["SSD-XXX"]
                else:
                    logger.warning(f"Missing parent {parent} for code {code}")
                    continue
            if desired_adminlevel == "1":
                admin_row = DBAdmin1(
                    location_ref=parent_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
            else:
                admin_row = DBAdmin2(
                    admin1_ref=parent_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
            self.session.add(admin_row)
            if i % self.limit == 0:
                self.session.commit()
        self.session.commit()
        # Create null relation rows with parents
        for parent_code, parent_ref in parent_dict[desired_adminlevel].items():
            if desired_adminlevel == "1":
                admin_row = DBAdmin1(
                    location_ref=parent_ref,
                    code=f"{parent_code}-XXX",
                    name="UNSPECIFIED",
                    is_unspecified=True,
                    # TODO: should this be made nullable?
                    #  Putting dummy data for now
                    reference_period_start=datetime(2000, 1, 1),
                )
            else:
                admin_row = DBAdmin2(
                    admin1_ref=parent_ref,
                    code=f"{parent_code}-XXX-XXX",
                    name="UNSPECIFIED",
                    is_unspecified=True,
                    reference_period_start=datetime(2000, 1, 1),
                )
            self.session.add(admin_row)
        self.session.commit()


class AdminFilter(AbstractStreamingFilter, ABC):
    def __init__(
        self,
        source: Dataset,
        desired_admin_level: str,
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
