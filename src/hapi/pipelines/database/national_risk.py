"""Functions specific to the national risk theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_national_risk import DBNationalRisk
from sqlalchemy.orm import Session

from . import locations
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)


class NationalRisk(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        locations: locations.Locations,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._locations = locations
        self._results = results

    def populate(self):
        logger.info("Populating national risk table")
        for dataset in self._results.values():
            datasetinfo = self._metadata.runner.scrapers[
                "national_risk_national"
            ].datasetinfo
            time_period_start = datasetinfo["source_date"]["default_date"][
                "start"
            ]
            time_period_end = datasetinfo["source_date"]["default_date"]["end"]
            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                hxl_tags = admin_results["headers"][1]
                locations = list(admin_results["values"][0].keys())
                values = {
                    hxl_tag: value
                    for hxl_tag, value in zip(
                        hxl_tags, admin_results["values"]
                    )
                }

                for location in locations:
                    risk_class = values["#risk+class"].get(location)
                    if risk_class:
                        risk_class = _get_risk_class_code_from_data(risk_class)

                    national_risk_row = DBNationalRisk(
                        resource_ref=self._metadata.resource_data[resource_id],
                        location_ref=self._locations.data[location],
                        risk_class=risk_class,
                        global_rank=values["#risk+rank"][location],
                        overall_risk=values["#risk+total"][location],
                        hazard_exposure_risk=values["#risk+hazard"][location],
                        vulnerability_risk=values["#risk+vulnerability"][
                            location
                        ],
                        coping_capacity_risk=values["#risk+coping+capacity"][
                            location
                        ],
                        meta_missing_indicators_pct=values[
                            "#meta+missing+indicators+pct"
                        ].get(location),
                        meta_avg_recentness_years=values[
                            "#meta+recentness+avg"
                        ].get(location),
                        reference_period_start=time_period_start,
                        reference_period_end=time_period_end,
                        # TODO: For v2+, add to scraper (HAPI-199)
                        source_data="not yet implemented",
                    )

                    self._session.add(national_risk_row)
        self._session.commit()


def _get_risk_class_code_from_data(risk_class: str) -> int:
    risk_class = risk_class.lower()
    risk_class_code = None
    if risk_class == "very high":
        risk_class_code = 5
    if risk_class == "high":
        risk_class_code = 4
    if risk_class == "medium":
        risk_class_code = 3
    if risk_class == "low":
        risk_class_code = 2
    if risk_class == "very low":
        risk_class_code = 1
    return risk_class_code
