"""Functions specific to the population theme."""

import re
from logging import getLogger
from typing import Dict, List

from sqlalchemy.orm import Session

from hapi.pipelines.database.db_age_range import DBAgeRange
from hapi.pipelines.database.db_population import DBPopulation
from hapi.pipelines.utilities.admins import Admins
from hapi.pipelines.utilities.metadata import Metadata

logger = getLogger(__name__)


def populate_population(
    results: Dict,
    session: Session,
    metadata: Metadata,
    admins: Admins,
    gender_code_list: List[str],
):
    logger.info("Populating population table")
    for result in results:
        resource_ref = metadata.data[result["resource"]["hdx_id"]]
        reference_period_start = result["reference_period"]["startdate"]
        reference_period_end = result["reference_period"]["enddate"]
        for hxl_tag, values in zip(result["headers"][1], result["values"]):
            if not _validate_hxl_tag(hxl_tag):
                raise ValueError(f"HXL tag {hxl_tag} not in valid format")
            gender_code, age_range_code = _get_hxl_mapping(hxl_tag=hxl_tag)
            if not _validate_gender_code(
                gender_code=gender_code, gender_code_list=gender_code_list
            ):
                raise ValueError(f"Gender code {gender_code} not in table")
            _populate_age_range(age_range_code, session)
            # TODO: add check that gender and age are in their
            #  respective tables
            for admin_code, value in values.items():
                population_row = DBPopulation(
                    resource_ref=resource_ref,
                    admin2_ref=admins.data[admin_code],
                    gender_code=gender_code,
                    age_range_code=age_range_code,
                    population=value,
                    reference_period_start=reference_period_start,
                    reference_period_end=reference_period_end,
                    # TODO: For v2+, add to scraper
                    source_data="not yet implemented",
                )

                session.add(population_row)
    session.commit()


def _validate_hxl_tag(hxl_tag: str) -> bool:
    # TODO: is this the right place to define the formats??
    """Validate HXL tags

    Assume they have the form:
        #population+total
        #population+f+total
        #population+ages_5_12+total
        #population+age_80_plus+total
        #population+f+age_5_12
        #population+f+age_80_plus
    """
    # TODO: test this
    pattern = r"^#population(\+[a-z])*(\+age_(\d+_\d+|\d+_plus))*(\+total)?$"
    return bool(re.match(pattern, hxl_tag))


def _get_hxl_mapping(hxl_tag: str) -> (str, str):
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


def _validate_gender_code(gender_code: str, gender_code_list) -> bool:
    return gender_code in gender_code_list or gender_code is None


def _populate_age_range(age_range_code: str, session: Session):
    """Add age range code to table if it doesn't yet exist."""
    # Check and exit if already exists
    if (
        age_range_code is None
        or session.query(DBAgeRange).filter_by(code=age_range_code).first()
    ):
        return
    ages = age_range_code.split("-")
    if len(ages) == 2:
        # Format: 0-5
        age_min, age_max = ages
    else:
        # Format: 80+
        age_min = age_range_code.replace("+", "")
        age_max = None
    age_range_row = DBAgeRange(
        code=age_range_code, age_min=age_min, age_max=age_max
    )
    session.add(age_range_row)
    session.commit()
