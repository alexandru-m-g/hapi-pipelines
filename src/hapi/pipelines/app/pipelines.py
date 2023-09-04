from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.app import population
from hapi.pipelines.database.dbpopulation import DBPopulation
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
        self.admins = Admins(session, self.locations)
        self.adminone = AdminLevel(configuration["admin1"], admin_level=1)

        Sources.set_default_source_date_format("%Y-%m-%d")
        self.runner = Runner(
            configuration["HRPs"],
            today,
            errors_on_exit=errors_on_exit,
            scrapers_to_run=scrapers_to_run,
        )
        self.configurable_scrapers = dict()

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
        _create_configurable_scrapers("adminone", adminlevel=self.adminone)

    def run(self):
        self.runner.run()

    def output(self):
        self.locations.populate()
        self.admins.populate()
        self.metadata.populate()

        # Get the population results and populate population table
        # TODO: what happens to the structure when other themes are included?
        #  Below it's written assuming admin1 population only,
        #  will need to be changed
        # TODO: How should the gender and age tables be populated?
        #  For now this is taken care of in the schema itself.
        #  For age in particular, should we populated it using the
        #  data or pre-define all values here in the codebase?
        results = self.runner.get_hapi_results()
        import pprint

        pp = pprint.PrettyPrinter(indent=2)
        pp.pprint(results)
        for result in results:
            for hxl_column, values in zip(
                result["headers"][1], result["values"]
            ):
                mappings = population.hxl_mapping[hxl_column]
                for admin_code, value in values.items():
                    # TODO: get the admin1 code for now, but
                    #  will need to change this to admin2
                    population_row = DBPopulation(
                        # TODO: Some open questions so filling with
                        #  fake data for now
                        resource_ref=self.metadata.data[
                            result["resource"]["code"]
                        ],
                        # TODO: get the admin1 code for now, but
                        #  will need to change this to admin2
                        admin2_ref=self.admins.data[admin_code],
                        gender_code=mappings.gender_code,
                        age_range_code=mappings.age_range_code,
                        population=value,
                        # TODO: These should also come from the metadata
                        reference_period_start=datetime(2000, 1, 1),
                        reference_period_end=datetime(2020, 1, 1),
                        # TODO: I suppose this should also somehow
                        #  come from the scraper?
                        source_data="pretend source data for now",
                    )

                    self.session.add(population_row)
        self.session.commit()
