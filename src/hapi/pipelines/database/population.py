"""Functions specific to the population theme."""

import re
from logging import getLogger
from typing import Dict

from hapi_schema.db_population import DBPopulation
from sqlalchemy.orm import Session

from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)

_HXL_PATTERN = re.compile(
    r"^#population(\+[a-z])*(\+age_(\d+_\d+|\d+_plus))*(\+total)?$"
)


class Population(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self._results = results

    def populate(self):
        logger.info("Populating population table")
        for dataset in self._results.values():
            time_period_start = dataset["time_period"]["start"]
            time_period_end = dataset["time_period"]["end"]

            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                for hxl_tag, values in zip(
                    admin_results["headers"][1], admin_results["values"]
                ):
                    if not _validate_gender_and_age_range_hxl_tag(hxl_tag):
                        raise ValueError(
                            f"HXL tag {hxl_tag} not in valid format"
                        )
                    gender, age_range = _get_gender_and_age_range_hxl_mapping(
                        hxl_tag=hxl_tag
                    )
                    if age_range is None:
                        age_range = "*"
                        min_age, max_age = None, None
                    else:
                        min_age, max_age = _get_age_min_and_max(age_range)
                    for admin_code, value in values.items():
                        admin2_code = admins.get_admin2_code_based_on_level(
                            admin_code=admin_code, admin_level=admin_level
                        )
                        population_row = DBPopulation(
                            resource_hdx_id=resource_id,
                            admin2_ref=self._admins.admin2_data[admin2_code],
                            gender=gender,
                            age_range=age_range,
                            min_age=min_age,
                            max_age=max_age,
                            population=int(value),
                            reference_period_start=time_period_start,
                            reference_period_end=time_period_end,
                        )

                        self._session.add(population_row)
        self._session.commit()


def _validate_gender_and_age_range_hxl_tag(hxl_tag: str) -> bool:
    """Validate HXL tags

    Assume they have the form:
        #population+total
        #population+f+total
        #population+age_5_12+total
        #population+age_80_plus+total
        #population+f+age_5_12
        #population+f+age_80_plus
    """
    # TODO: add tests for this (HAPI-159)
    return bool(_HXL_PATTERN.match(hxl_tag))


def _get_gender_and_age_range_hxl_mapping(hxl_tag: str) -> (str, str):
    components = hxl_tag.split("+")
    gender = None
    age_range = None
    for component in components[1:]:
        # components can only be age, gender, or the word "total"
        if component.startswith("age_"):
            age_component = component[4:]
            if age_component.endswith("_plus"):
                age_range = age_component[:-5] + "+"
            else:
                age_range = age_component.replace("_", "-")
        elif component != "total":
            gender = component
    return gender, age_range


def _get_age_min_and_max(age_range: str) -> (int, int):
    ages = age_range.split("-")
    if len(ages) == 2:
        # Format: 0-5
        min_age, max_age = int(ages[0]), int(ages[1])
    else:
        # Format: 80+
        min_age = int(age_range.replace("+", ""))
        max_age = None
    return min_age, max_age
