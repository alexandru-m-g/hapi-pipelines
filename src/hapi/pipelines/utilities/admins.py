import logging
from typing import Dict

import hxl
from hdx.utilities.dateparse import parse_date
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_admin1 import DBAdmin1
from hapi.pipelines.database.db_admin2 import DBAdmin2
from hapi.pipelines.utilities.locations import Locations

logger = logging.getLogger(__name__)


class Admins:
    def __init__(
        self,
        configuration: Dict,
        session: Session,
        locations: Locations,
        libhxl_dataset: hxl.Dataset,
    ):
        self.limit = configuration["commit_limit"]
        self.session = session
        self.locations = locations
        self.libhxl_dataset = libhxl_dataset
        self.data = {}

    def populate(self):
        def update_admin_table(desired_adminlevel):
            for i, row in enumerate(
                self.libhxl_dataset.with_rows(
                    f"#geo+admin_level={desired_adminlevel}"
                )
            ):
                code = row.get("#adm+code")
                name = row.get("#adm+name")
                reference_period_start = parse_date(row.get("#date+start"))
                parent = row.get("#adm+code+parent")
                if desired_adminlevel == "1":
                    location_ref = self.locations.data.get(parent)
                    if location_ref:
                        admin_row = DBAdmin1(
                            location_ref=location_ref,
                            code=code,
                            name=name,
                            reference_period_start=reference_period_start,
                        )
                else:
                    admin_ref = self.data.get(parent)
                    if admin_ref:
                        admin_row = DBAdmin2(
                            admin1_ref=admin_ref,
                            code=code,
                            name=name,
                            reference_period_start=reference_period_start,
                        )
                self.session.add(admin_row)
                if i % self.limit == 0:
                    self.session.commit()
            self.session.commit()

        update_admin_table("1")
        results = self.session.execute(select(DBAdmin1.id, DBAdmin1.code))
        self.data = {result[1]: result[0] for result in results}
        update_admin_table("2")
        results = self.session.execute(select(DBAdmin2.id, DBAdmin2.code))
        for result in results:
            self.data[result[1]] = result[0]
