"""Functions specific to the national risk theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_national_risk import DBNationalRisk
from hdx.utilities.dictandlist import dict_of_dicts_add
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
            reference_period_start = datasetinfo["source_date"][
                "default_date"
            ]["start"]
            reference_period_end = datasetinfo["source_date"]["default_date"][
                "end"
            ]
            for admin_level, admin_results in dataset["results"].items():
                rows = dict()
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                for hxl_tag, values in zip(
                    admin_results["headers"][1], admin_results["values"]
                ):
                    for admin_code, value in values.items():
                        if hxl_tag == "#risk+class":
                            value = _get_risk_class_code_from_data(value)
                        dict_of_dicts_add(rows, admin_code, hxl_tag, value)
                for location in rows:
                    national_risk_row = DBNationalRisk(
                        resource_ref=self._metadata.resource_data[resource_id],
                        location_ref=self._locations.data[location],
                        risk_class=rows[location]["#risk+class"],
                        global_rank=rows[location]["#risk+rank"],
                        overall_risk=rows[location]["#risk+total"],
                        hazard_exposure_risk=rows[location]["#risk+hazard"],
                        vulnerability_risk=rows[location][
                            "#risk+vulnerability"
                        ],
                        coping_capacity_risk=rows[location][
                            "#risk+coping+capacity"
                        ],
                        meta_missing_indicators_pct=rows[location].get(
                            "#meta+missing+indicators+pct"
                        ),
                        meta_avg_recentness_years=rows[location].get(
                            "#meta+recentness+avg"
                        ),
                        reference_period_start=reference_period_start,
                        reference_period_end=reference_period_end,
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
