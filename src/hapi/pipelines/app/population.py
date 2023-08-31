"""Functions specific to the population theme."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PopulationEntry:
    gender_code: Optional[str]
    age_range_code: Optional[str]


hxl_mapping = {
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
