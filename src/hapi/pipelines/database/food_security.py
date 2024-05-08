"""Functions specific to the food security theme."""

from datetime import datetime
from logging import getLogger
from typing import Dict

from hapi_schema.db_food_security import DBFoodSecurity
from hapi_schema.utils.enums import IPCPhase
from hdx.utilities.dateparse import parse_date_range
from sqlalchemy.orm import Session

from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)


class FoodSecurity(BaseUploader):
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
        logger.info("Populating food security table")
        for dataset in self._results.values():
            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                # Get all the column positions
                column_names = admin_results["headers"][0]
                ipc_type_column = column_names.index("ipc_type")
                reference_period_months_column = column_names.index(
                    "reference_period_months"
                )
                reference_period_year_column = column_names.index(
                    "reference_period_year"
                )
                population_in_phase_columns = {
                    "1": column_names.index("population_phase1"),
                    "2": column_names.index("population_phase2"),
                    "3": column_names.index("population_phase3"),
                    "4": column_names.index("population_phase4"),
                    "5": column_names.index("population_phase5"),
                    "3+": column_names.index("population_phase3+"),
                    "*": column_names.index("population_total"),
                }
                # Loop through each pcode
                values = admin_results["values"]
                for admin_code in values[0].keys():
                    admin2_code = admins.get_admin2_code_based_on_level(
                        admin_code=admin_code, admin_level=admin_level
                    )
                    admin2_ref = self._admins.admin2_data[admin2_code]
                    # Loop through all entries in each pcode
                    for irow in range(len(values[0][admin_code])):
                        ipc_type = _get_ipc_type_code_from_data(
                            ipc_type_from_data=values[ipc_type_column][
                                admin_code
                            ][irow]
                        )
                        (
                            time_period_start,
                            time_period_end,
                        ) = _get_time_period(
                            month_range=values[reference_period_months_column][
                                admin_code
                            ][irow],
                            year=values[reference_period_year_column][
                                admin_code
                            ][irow],
                        )
                        # Total population required to calculate fraction in phase
                        population_total = int(
                            values[population_in_phase_columns["*"]][
                                admin_code
                            ][irow]
                        )
                        for ipc_phase in IPCPhase:
                            population_in_phase = values[
                                population_in_phase_columns[ipc_phase.value]
                            ][admin_code][irow]
                            if population_in_phase is None:
                                population_in_phase = 0
                            population_in_phase = int(population_in_phase)
                            food_security_row = DBFoodSecurity(
                                resource_hdx_id=resource_id,
                                admin2_ref=admin2_ref,
                                ipc_phase=ipc_phase.value,
                                ipc_type=ipc_type,
                                reference_period_start=time_period_start,
                                reference_period_end=time_period_end,
                                population_in_phase=population_in_phase,
                                population_fraction_in_phase=(
                                    population_in_phase / population_total
                                    if population_in_phase
                                    else 0.0
                                ),
                            )
                            self._session.add(food_security_row)
        self._session.commit()


def _get_ipc_type_code_from_data(ipc_type_from_data: str) -> str:
    mapping = {
        "current": "current",
        "projected": "first projection",
    }
    try:
        return mapping[ipc_type_from_data]
    except KeyError as e:
        raise KeyError(
            f"IPC type {ipc_type_from_data} not found in mapping"
        ) from e


def _get_time_period(month_range: str, year: str) -> (datetime, datetime):
    time_period_start = parse_date_range(
        f"{year} {month_range.split('-')[0]}"
    )[0]
    time_period_end = parse_date_range(f"{year} {month_range.split('-')[1]}")[
        1
    ]
    return time_period_start, time_period_end
