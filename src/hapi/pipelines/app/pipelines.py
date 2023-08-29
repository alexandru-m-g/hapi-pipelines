from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.app.locations import Locations


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
        self.countries = Locations(configuration, use_live)
        self.adminone = AdminLevel(configuration["admin1"], admin_level=1)

        Sources.set_default_source_date_format("%Y-%m-%d")
        self.runner = Runner(
            configuration["countries"],
            today,
            errors_on_exit=errors_on_exit,
            scrapers_to_run=scrapers_to_run,
        )
        self.configurable_scrapers = dict()

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
        # Locations

        self.runner.get_results()
        #  Transform and write the results to population schema in db
        #  We need mapping from HXL hashtags in results to gender and age range codes

        self.runner.get_hapi_metadata()
        # Gets Datasets and Resources
