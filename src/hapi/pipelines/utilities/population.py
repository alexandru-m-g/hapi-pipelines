"""Functions specific to the population theme."""

from dataclasses import dataclass
from typing import Dict, Optional

from sqlalchemy.orm import Session

from hapi.pipelines.database.db_population import DBPopulation
from hapi.pipelines.utilities.admins import Admins
from hapi.pipelines.utilities.metadata import Metadata


@dataclass
class PopulationEntry:
    gender_code: Optional[str]
    age_range_code: Optional[str]


_HXL_MAPPING = {
    "#population+total": PopulationEntry(
        gender_code=None, age_range_code=None
    ),
    "#population+f+total": PopulationEntry(
        gender_code="f", age_range_code=None
    ),
    "#population+m+total": PopulationEntry(
        gender_code="m", age_range_code=None
    ),
}


def populate_population(
    results: Dict, session: Session, metadata: Metadata, admins: Admins
):
    for result in results:
        resource_ref = metadata.data[result["resource"]["code"]]
        reference_period_start = result["reference_period"]["startdate"]
        reference_period_end = result["reference_period"]["enddate"]
        for hxl_column, values in zip(result["headers"][1], result["values"]):
            mappings = _HXL_MAPPING[hxl_column]
            for admin_code, value in values.items():
                population_row = DBPopulation(
                    resource_ref=resource_ref,
                    # TODO: get the admin1 code for now, but
                    #  will need to change this to admin2
                    admin2_ref=admins.data[admin_code],
                    gender_code=mappings.gender_code,
                    age_range_code=mappings.age_range_code,
                    population=value,
                    reference_period_start=reference_period_start,
                    reference_period_end=reference_period_end,
                    # TODO: Add to scraper
                    source_data="not yet implemented",
                )

                session.add(population_row)
    session.commit()
