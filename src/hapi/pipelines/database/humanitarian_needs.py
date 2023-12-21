"""Functions specific to the humanitarian needs theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_humanitarian_needs import DBHumanitarianNeeds
from hxl.model import Column, TagPattern
from sqlalchemy.orm import Session

from . import admins
from .age_range import AgeRange
from .base_uploader import BaseUploader
from .gender import Gender
from .metadata import Metadata
from .population_group import PopulationGroup
from .population_status import PopulationStatus
from .sector import Sector

logger = getLogger(__name__)


class HumanitarianNeeds(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        gender: Gender,
        age_range: AgeRange,
        sector: Sector,
        population_group: PopulationGroup,
        population_status: PopulationStatus,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self.gender_patterns = gender.patterns
        self.age_range_patterns = age_range.patterns
        self.disabled_pattern = TagPattern.parse("#*+disabled")
        self.sector_patterns = sector.patterns
        self.population_group_patterns = population_group.patterns
        self.population_status_patterns = population_status.patterns
        self._results = results

    def populate(self):
        logger.info("Populating humanitarian needs table")

        def match_column(col, patterns):
            for pattern in patterns:
                if pattern.match(col):
                    result = pattern.tag
                    if result:
                        return result[1:]
                    result = pattern.include_attributes
                    if result:
                        return result.pop()[1:]
                    break
            return None

        for dataset in self._results.values():
            reference_period_start = dataset["reference_period"]["startdate"]
            reference_period_end = dataset["reference_period"]["enddate"]

            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                for hxl_tag, values in zip(
                    admin_results["headers"][1], admin_results["values"]
                ):
                    column = Column.parse(hxl_tag)
                    # "#inneed" "#affected"
                    population_status_code = match_column(
                        column, self.population_status_patterns
                    )
                    if not population_status_code:
                        raise ValueError(f"Invalid HXL tag {hxl_tag}!")
                    # "#*+f" "#*+m"
                    gender_code = match_column(column, self.gender_patterns)
                    # "#*+age0_4" "#*+age80plus"
                    age_range_code = match_column(
                        column, self.age_range_patterns
                    )
                    # "#*+disabled"
                    disabled_marker = self.disabled_pattern.match(column)
                    if not disabled_marker:
                        disabled_marker = None  # no disabled attribute
                    # TODO: Will there be columns for able bodied?
                    # "#*+wsh" "#*+pro_gbv"
                    sector_code = match_column(column, self.sector_patterns)
                    # "#*+idps" "#*+refugees"
                    population_group_code = match_column(
                        column, self.population_group_patterns
                    )
                    for admin_code, value in values.items():
                        admin2_code = admins.get_admin2_code_based_on_level(
                            admin_code=admin_code, admin_level=admin_level
                        )
                        humanitarian_needs_row = DBHumanitarianNeeds(
                            resource_ref=self._metadata.resource_data[
                                resource_id
                            ],
                            admin2_ref=self._admins.admin2_data[admin2_code],
                            gender_code=gender_code,
                            age_range_code=age_range_code,
                            disabled_marker=disabled_marker,
                            sector_code=sector_code,
                            population_group_code=population_group_code,
                            population_status_code=population_status_code,
                            population=int(value),
                            reference_period_start=reference_period_start,
                            reference_period_end=reference_period_end,
                            # TODO: For v2+, add to scraper (HAPI-199)
                            source_data="not yet implemented",
                        )

                        self._session.add(humanitarian_needs_row)
        self._session.commit()
