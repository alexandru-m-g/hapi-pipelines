import logging

import hxl
from hdx.utilities.dateparse import parse_date
from hxl import InputOptions
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
                "https://data.humdata.org/dataset/global-pcodes",
                InputOptions(encoding="utf-8"),
            )
        except OSError:
            logger.exception("Download of admin info from dataset failed!")
            return

        for row in admin_info:
            adminlevel = row.get("#geo+admin_level")
            if adminlevel not in ("1", "2"):
                continue
            code = row.get("#adm+code")
            name = row.get("#adm+name")
            reference_period_start = parse_date(row.get("#date+start"))
            parent = row.get("#adm+code+parent")
            if adminlevel == "1":
                location_ref = self.locations.data[parent]
                admin_row = DBAdmin1(
                    location_ref=location_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
                self.session.add(admin_row)
                self.session.commit()
                self.data[code] = admin_row.id
            else:
                admin_ref = self.data[parent]
                admin_row = DBAdmin2(
                    admin1_ref=admin_ref,
                    code=code,
                    name=name,
                    reference_period_start=reference_period_start,
                )
            self.session.add(admin_row)
        self.session.commit()
