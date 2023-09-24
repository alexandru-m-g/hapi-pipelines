from typing import Dict

from hapi_schema.db_location import DBLocation
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from sqlalchemy.orm import Session


class Locations:
    def __init__(
        self, configuration: Dict, session: Session, use_live: bool = True
    ):
        Country.countriesdata(
            use_live=use_live,
            country_name_overrides=configuration["country_name_overrides"],
            country_name_mappings=configuration["country_name_mappings"],
        )
        self.hrps = configuration["HRPs"]
        self.session = session
        self.data = {}

    def populate(self):
        for country in Country.countriesdata()["countries"].values():
            code = country["#country+code+v_iso3"]
            if code not in self.hrps:
                continue
            location_row = DBLocation(
                code=code,
                name=country["#country+name+preferred"],
                reference_period_start=parse_date(country["#date+start"]),
            )
            self.session.add(location_row)
            self.session.commit()
            self.data[code] = location_row.id
