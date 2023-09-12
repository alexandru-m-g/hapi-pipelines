from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.app import population
from hapi.pipelines.database.db_population import DBPopulation
from hapi.pipelines.utilities.admins import Admins
from hapi.pipelines.utilities.locations import Locations
from hapi.pipelines.utilities.metadata import Metadata


class Pipelines:
    def __init__(
        self,
        configuration: Dict,
        session: Session,
        today: datetime,
        scrapers_to_run: Optional[ListTuple[str]] = None,
        errors_on_exit: Optional[ErrorsOnExit] = None,
        use_live: bool = True,
        fallbacks_root: Optional[str] = None,
    ):
        self.configuration = configuration
        self.session = session
        self.locations = Locations(configuration, session, use_live)
        libhxl_dataset = AdminLevel.get_libhxl_dataset().cache()
        self.admins = Admins(
            configuration, session, self.locations, libhxl_dataset
        )
        self.admintwo = AdminLevel(admin_level=2)
        self.admintwo.setup_from_libhxl_dataset(libhxl_dataset)

        Sources.set_default_source_date_format("%Y-%m-%d")
        self.runner = Runner(
            configuration["HRPs"],
            today,
            errors_on_exit=errors_on_exit,
            scrapers_to_run=scrapers_to_run,
        )
        self.configurable_scrapers = dict()
        self.metadata = Metadata(runner=self.runner, session=session)

        self.metadata = Metadata(runner=self.runner, session=session)

        if fallbacks_root is not None:
            pass
        self.create_configurable_scrapers()

    def create_configurable_scrapers(self):
        def _create_configurable_scrapers(
            level, suffix_attribute=None, adminlevel=None
        ):
            suffix = f"_{level}"
            source_configuration = Sources.create_source_configuration(
                suffix_attribute=suffix_attribute,
                admin_sources=True,
                adminlevel=adminlevel,
            )
            self.configurable_scrapers[level] = self.runner.add_configurables(
                self.configuration[f"scraper{suffix}"],
                level,
                adminlevel=adminlevel,
                source_configuration=source_configuration,
                suffix=suffix,
            )

        _create_configurable_scrapers("national")
        _create_configurable_scrapers("admintwo", adminlevel=self.admintwo)

    def run(self):
        self.runner.run()

    def output(self):
        self.locations.populate()
        self.admins.populate()
        self.metadata.populate()
        #  Below it's written assuming admin1 population only,
        #  will need to be changed
        results = self.runner.get_hapi_results()
        for result in results:
            resource_ref = self.metadata.data[result["resource"]["code"]]
            reference_period_start = result["reference_period"]["startdate"]
            reference_period_end = result["reference_period"]["enddate"]
            for hxl_column, values in zip(
                result["headers"][1], result["values"]
            ):
                mappings = population.hxl_mapping[hxl_column]
                for admin_code, value in values.items():
                    population_row = DBPopulation(
                        resource_ref=resource_ref,
                        # TODO: get the admin1 code for now, but
                        #  will need to change this to admin2
                        admin2_ref=self.admins.data[admin_code],
                        gender_code=mappings.gender_code,
                        age_range_code=mappings.age_range_code,
                        population=value,
                        reference_period_start=reference_period_start,
                        reference_period_end=reference_period_end,
                        # TODO: Add to scraper
                        source_data="not yet implemented",
                    )

                    self.session.add(population_row)
        self.session.commit()
