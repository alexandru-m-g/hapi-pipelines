from typing import Dict

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date


class Locations:
    def __init__(self, configuration: Dict, use_live: bool = True):
        Country.countriesdata(
            use_live=use_live,
            country_name_overrides=configuration["country_name_overrides"],
            country_name_mappings=configuration["country_name_mappings"],
        )
        self.data = []

    def populate(self):
        for country in Country.countriesdata()["countries"].values():
            country["#country+code+v_iso3"]
            country["#country+name+preferred"]
            parse_date(country["#date+start"])
            # add to db
