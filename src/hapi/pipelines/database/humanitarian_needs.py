"""Functions specific to the humanitarian needs theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_humanitarian_needs import DBHumanitarianNeeds
from hxl.model import Column, TagPattern
from sqlalchemy.orm import Session

from ..utilities.parse_tags import (
    get_gender_and_age_range,
    get_min_and_max_age,
)
from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata
from .sector import Sector

logger = getLogger(__name__)


class HumanitarianNeeds(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        sector: Sector,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self.sector_pattern_to_code = sector.pattern_to_code
        self._results = results

    def populate(self):
        logger.info("Populating humanitarian needs table")

        for dataset in self._results.values():
            time_period_start = dataset["time_period"]["start"]
            time_period_end = dataset["time_period"]["end"]

            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                for hxl_tag, values in zip(
                    admin_results["headers"][1], admin_results["values"]
                ):
                    column = Column.parse(hxl_tag)
                    # "#inneed" "#affected"
                    population_status = _get_population_status(column)
                    if not population_status:
                        raise ValueError(f"Invalid HXL tag {hxl_tag}!")
                    # "#*+idps" "#*+refugees"
                    population_group = _get_population_group(column)
                    # "#*+wsh" "#*+pro_gbv"
                    sector_code = match_column(
                        column, self.sector_pattern_to_code
                    )
                    if not sector_code:
                        sector_code = "*"
                    sector_code = sector_code.upper()
                    # "#*+age0_4" "#*+age80plus"
                    gender, age_range = get_gender_and_age_range(hxl_tag)
                    min_age, max_age = get_min_and_max_age(age_range)
                    # "#*+disabled"
                    disabled_marker = _get_disabled_marker(column)
                    # TODO: Will there be columns for able bodied?
                    for admin_code, value in values.items():
                        try:
                            value = int(value)
                        except (ValueError, TypeError):
                            continue
                        admin2_code = admins.get_admin2_code_based_on_level(
                            admin_code=admin_code, admin_level=admin_level
                        )
                        humanitarian_needs_row = DBHumanitarianNeeds(
                            resource_hdx_id=resource_id,
                            admin2_ref=self._admins.admin2_data[admin2_code],
                            gender=gender,
                            age_range=age_range,
                            min_age=min_age,
                            max_age=max_age,
                            sector_code=sector_code,
                            population_group=population_group,
                            population_status=population_status,
                            disabled_marker=disabled_marker,
                            population=value,
                            reference_period_start=time_period_start,
                            reference_period_end=time_period_end,
                        )

                        self._session.add(humanitarian_needs_row)
        self._session.commit()


def match_column(col: Column, pattern_to_code: Dict) -> str | None:
    for pattern in pattern_to_code:
        if pattern.match(col):
            return pattern_to_code[pattern]
    return None


def _get_population_status(col: Column) -> str:
    population_status_patterns = {
        TagPattern.parse("#population"): "POP",
        TagPattern.parse("#affected"): "AFF",
        TagPattern.parse("#inneed"): "INN",
        TagPattern.parse("#targeted"): "TGT",
        TagPattern.parse("#reached"): "REA",
    }
    population_status = match_column(col, population_status_patterns)
    return population_status


def _get_population_group(col: Column) -> str:
    population_group_patterns = {
        TagPattern.parse("#*+refugees"): "REF",
        TagPattern.parse("#*+returnees"): "RET",
        TagPattern.parse("#*+idps"): "IDP",
    }
    population_group = match_column(col, population_group_patterns)
    if not population_group:
        population_group = "*"
    return population_group


def _get_disabled_marker(col: Column) -> str:
    disabled_marker = TagPattern.parse("#*+disabled").match(col)
    if disabled_marker:
        return "y"
    if not disabled_marker:
        return "*"
