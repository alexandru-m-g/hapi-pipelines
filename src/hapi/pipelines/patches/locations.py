from datetime import datetime
from typing import Dict

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from src.hapi.pipelines.utilities.hapi_patch import HAPIPatch

from .base_uploader import BaseUploader


class Locations(BaseUploader):
    def __init__(
        self, configuration: Dict, today: datetime, use_live: bool = True
    ):
        super().__init__(configuration["hapi_repo"])
        Country.countriesdata(
            use_live=use_live,
            country_name_overrides=configuration["country_name_overrides"],
            country_name_mappings=configuration["country_name_mappings"],
        )
        self._hapi_countries = configuration["HAPI_countries"]
        self._today = today
        self.data = {}

    def generate_hapi_patch(self):
        with HAPIPatch(self._hapi_repo) as hapi_patch:
            values = []
            for country in Country.countriesdata()["countries"].values():
                code = country["#country+code+v_iso3"]
                if code not in self._hapi_countries:
                    continue
                values.append(
                    [
                        code,
                        country["#country+name+preferred"],
                        parse_date(country["#date+start"]),
                    ]
                )

            patch = {
                "description": "Initial population of locations",
                "sequence": hapi_patch.get_sequence_number(),
                "database_schema_version": "0.7.0",
                "changes": [
                    {
                        "type": "INSERT",
                        "entity": "DBLocation",
                        "headers": [
                            "code",
                            "name",
                            "reference_period_start",
                            {
                                "name": "hapi_updated_date",
                                "value": self._today,
                            },
                        ],
                        "values": values,
                    }
                ],
            }
            hapi_patch.create("hno", patch)
