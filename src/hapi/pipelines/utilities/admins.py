import logging

import hxl
from hdx.utilities.dateparse import parse_date
from hxl import InputOptions
from sqlalchemy import select
from sqlalchemy.orm import Session

from hapi.pipelines.database.dbadmin1 import DBAdmin1
from hapi.pipelines.database.dbadmin2 import DBAdmin2
from hapi.pipelines.utilities.locations import Locations

logger = logging.getLogger(__name__)


class Admins:
    def __init__(self, session: Session, locations: Locations):
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

        def update_admin_table(desired_adminlevel):
            for i, row in enumerate(admin_info):
                adminlevel = row.get("#geo+admin_level")
                if adminlevel != desired_adminlevel:
                    continue
                code = row.get("#adm+code")
                name = row.get("#adm+name")
                reference_period_start = parse_date(row.get("#date+start"))
                parent = row.get("#adm+code+parent")
                if desired_adminlevel == "1":
                    location_ref = self.locations.data[parent]
                    admin_row = DBAdmin1(
                        location_ref=location_ref,
                        code=code,
                        name=name,
                        reference_period_start=reference_period_start,
                    )
                else:
                    admin_ref = self.data[parent]
                    admin_row = DBAdmin2(
                        admin1_ref=admin_ref,
                        code=code,
                        name=name,
                        reference_period_start=reference_period_start,
                    )
                self.session.add(admin_row)
                if i % 1000 == 0:
                    self.session.commit()
            self.session.commit()

        update_admin_table("1")
        select(DBAdmin1.id, DBAdmin1.code)
        results = self.session.execute(select(DBAdmin1.id, DBAdmin1.code))
        for result in results:
            self.data[result[1]] = result[0]
        update_admin_table("2")
        results = self.session.execute(select(DBAdmin2.id, DBAdmin2.code))
        for result in results:
            self.data[result[1]] = result[0]
