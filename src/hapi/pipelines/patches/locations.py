from dataclasses import dataclass
from datetime import datetime
from typing import Dict

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from .base_uploader import BaseUploader
from hapi.pipelines.utilities.hapi_patch import HAPIPatch
from hapi.pipelines.utilities.ident_creator import generate_random_md5


@dataclass
class LocationData:
    ident: str
    code: str
    reference_period_start: datetime


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
        self.code_location_data_dict = {}

    def generate_hapi_patch(self):
        rows = []
        for country in Country.countriesdata()["countries"].values():
            code = country["#country+code+v_iso3"]
            if code not in self._hapi_countries:
                continue
            ident = generate_random_md5()
            reference_period_start = parse_date(country["#date+start"])
            rows.append(
                [
                    ident,
                    code,
                    country["#country+name+preferred"],
                    reference_period_start.isoformat(),
                ]
            )
            # TODO: this is not going to work for versioning,
            #  but maybe that's okay?
            self.code_location_data_dict[code] = LocationData(
                code=code,
                ident=ident,
                reference_period_start=reference_period_start,
            )

        with HAPIPatch(self._hapi_repo) as hapi_patch:
            patch = {
                "description": "Initial population of locations",
                "sequence": hapi_patch.get_sequence_number(),
                "database_schema_version": "0.7.0",
                "changes": [
                    {
                        "type": "INSERT",
                        "entity": "DBLocation",
                        "headers": [
                            "ident",
                            "code",
                            "name",
                            "reference_period_start",
                            {
                                "name": "hapi_updated_date",
                                "value": self._today.isoformat(),
                            },
                        ],
                        "values": rows,
                    }
                ],
            }
            hapi_patch.create(theme="location", patch=patch)
