from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.database.admins import Admins
from hapi.pipelines.database.age_range import AgeRange
from hapi.pipelines.database.food_security import FoodSecurity
from hapi.pipelines.database.gender import Gender
from hapi.pipelines.database.ipc_phase import IpcPhase
from hapi.pipelines.database.ipc_type import IpcType
from hapi.pipelines.database.locations import Locations
from hapi.pipelines.database.metadata import Metadata
from hapi.pipelines.database.operational_presence import OperationalPresence
from hapi.pipelines.database.org import Org
from hapi.pipelines.database.org_type import OrgType
from hapi.pipelines.database.population import Population
from hapi.pipelines.database.sector import Sector


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
            session=session,
            datasetinfo=configuration["org_type"],
            org_type_map=configuration["org_type_map"],
        )
        self.sector = Sector(
            session=session,
            datasetinfo=configuration["sector"],
            sector_map=configuration["sector_map"],
        )
        self.gender = Gender(
            session=session,
            gender_descriptions=configuration["gender_descriptions"],
        )
        self.age_range = AgeRange(session=session)

        self.ipc_phase = IpcPhase(
            session=session,
            ipc_phase_names=configuration["ipc_phase_names"],
            ipc_phase_descriptions=configuration["ipc_phase_descriptions"],
        )
        self.ipc_type = IpcType(
            session=session,
            ipc_type_descriptions=configuration["ipc_type_descriptions"],
        )

        Sources.set_default_source_date_format("%Y-%m-%d")
        self.runner = Runner(
            configuration["HAPI_countries"],
            today,
            errors_on_exit=errors_on_exit,
            scrapers_to_run=scrapers_to_run,
        )
        self.configurable_scrapers = dict()
        self.create_configurable_scrapers()
        self.metadata = Metadata(runner=self.runner, session=session)

    def create_configurable_scrapers(self):
        def _create_configurable_scrapers(
            prefix, level, suffix_attribute=None, adminlevel=None
        ):
            suffix = f"_{level}"
            source_configuration = Sources.create_source_configuration(
                suffix_attribute=suffix_attribute,
                admin_sources=True,
                adminlevel=adminlevel,
            )
            scrapers = self.runner.add_configurables(
                self.configuration[f"{prefix}{suffix}"],
                level,
                adminlevel=adminlevel,
                source_configuration=source_configuration,
                suffix=suffix,
            )
            current_scrapers = self.configurable_scrapers.get(prefix, [])
            self.configurable_scrapers[prefix] = current_scrapers + scrapers

        _create_configurable_scrapers("population", "national")
        _create_configurable_scrapers(
            "population", "adminone", adminlevel=self.adminone
        )
        _create_configurable_scrapers(
            "population", "admintwo", adminlevel=self.admintwo
        )
        _create_configurable_scrapers(
            "operational_presence", "admintwo", adminlevel=self.admintwo
        )
        _create_configurable_scrapers(
            "food_security", "adminone", adminlevel=self.adminone
        )
        _create_configurable_scrapers(
            "food_security", "admintwo", adminlevel=self.admintwo
        )

    def run(self):
        self.runner.run()

    def output(self):
        self.locations.populate()
        self.admins.populate()
        self.metadata.populate()
        self.org_type.populate()
        self.sector.populate()
        self.gender.populate()
        self.ipc_phase.populate()
        self.ipc_type.populate()

        results = self.runner.get_hapi_results(
            self.configurable_scrapers["population"]
        )

        population = Population(
            session=self.session,
            metadata=self.metadata,
            admins=self.admins,
            gender=self.gender,
            age_range=self.age_range,
            results=results,
        )
        population.populate()

        results = self.runner.get_hapi_results(
            self.configurable_scrapers["operational_presence"]
        )
        operational_presence = OperationalPresence(
            session=self.session,
            metadata=self.metadata,
            admins=self.admins,
            org=self.org,
            org_type=self.org_type,
            sector=self.sector,
            results=results,
        )
        operational_presence.populate()

        results = self.runner.get_hapi_results(
            self.configurable_scrapers["food_security"]
        )
        food_security = FoodSecurity(
            session=self.session,
            metadata=self.metadata,
            admins=self.admins,
            ipc_phase=self.ipc_phase,
            ipc_type=self.ipc_type,
            results=results,
        )
        food_security.populate()
