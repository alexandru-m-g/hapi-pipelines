from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.utilities.admins import Admins
from hapi.pipelines.utilities.age_range import AgeRange
from hapi.pipelines.utilities.gender import Gender
from hapi.pipelines.utilities.locations import Locations
from hapi.pipelines.utilities.metadata import Metadata
from hapi.pipelines.utilities.operational_presence import OperationalPresence
from hapi.pipelines.utilities.org import Org
from hapi.pipelines.utilities.org_type import OrgType
from hapi.pipelines.utilities.population import Population
from hapi.pipelines.utilities.sector import Sector


class Pipelines:
    def __init__(
        self,
        configuration: Dict,
        session: Session,
        today: datetime,
        scrapers_to_run: Optional[ListTuple[str]] = None,
        errors_on_exit: Optional[ErrorsOnExit] = None,
        use_live: bool = True,
    ):
        self.configuration = configuration
        self.session = session
        self.locations = Locations(
            configuration=configuration, session=session, use_live=use_live
        )
        libhxl_dataset = AdminLevel.get_libhxl_dataset().cache()
        self.admins = Admins(
            configuration, session, self.locations, libhxl_dataset
        )
        self.adminone = AdminLevel(admin_level=1)
        self.admintwo = AdminLevel(admin_level=2)
        self.adminone.setup_from_libhxl_dataset(libhxl_dataset)
        self.admintwo.setup_from_libhxl_dataset(libhxl_dataset)

        self.org = Org(session=session)
        self.org_type = OrgType(
            session=session, datasetinfo=configuration["org_type"]
        )
        self.sector = Sector(
            session=session, datasetinfo=configuration["sector"]
        )
        # TODO: make this a single scraper once metadata issue is solved
        self.operational_presence = [
            OperationalPresence(
                country_code=country_code.lower(),
                session=session,
                datasetinfo=configuration[
                    f"operational_presence_{country_code.lower()}"
                ],
                admins=self.admins,
                org=self.org,
                org_type=self.org_type,
                sector=self.sector,
                sector_map=configuration["sector_map"],
                org_type_map=configuration["org_type_map"],
            )
            for country_code in self.configuration["HAPI_countries"]
        ]
        self.gender = Gender(
            session=session,
            gender_descriptions=configuration["gender_descriptions"],
        )
        self.age_range = AgeRange(session=session)

        Sources.set_default_source_date_format("%Y-%m-%d")
        self.runner = Runner(
            configuration["HAPI_countries"],
            today,
            errors_on_exit=errors_on_exit,
            scrapers_to_run=scrapers_to_run,
        )
        self.configurable_scrapers = dict()
        self.create_configurable_scrapers()
        self.runner.add_customs(
            (self.org_type, self.sector, *self.operational_presence)
        )
        self.metadata = Metadata(runner=self.runner, session=session)

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
        _create_configurable_scrapers("admintwo", adminlevel=self.admintwo)

    def run(self):
        self.runner.run()

    def output(self):
        self.locations.populate()
        self.admins.populate()
        # TODO: Add hapi metadata from 3W (doesn't currently work as it's multiple datasets)
        self.metadata.populate()
        self.org_type.populate()
        self.sector.populate()
        # TODO: make this a single scraper once metadata issue is solved
        for scraper in self.operational_presence:
            scraper.populate(metadata=self.metadata)
        self.gender.populate()
        results = self.runner.get_hapi_results()
        population = Population(
            session=self.session,
            metadata=self.metadata,
            admins=self.admins,
            gender=self.gender,
            age_range=self.age_range,
        )
        population.populate(results=results)
