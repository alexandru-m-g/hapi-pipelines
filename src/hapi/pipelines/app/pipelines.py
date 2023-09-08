from datetime import datetime
from typing import Dict, Optional

import hxl
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from hxl import InputOptions
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_dataset import DBDataset
from hapi.pipelines.database.db_resource import DBResource
from hapi.pipelines.utilities.admins import Admins
from hapi.pipelines.utilities.locations import Locations


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
        # Gets Datasets and Resources
        hapi_results = self.runner.get_hapi_results()
        for dataset in hapi_results:
            resource = dataset["resource"]

            dataset_row = DBDataset(
                hdx_link=dataset["hdx_link"],
                code=dataset["code"],
                title=dataset["title"],
                provider_code=dataset["provider_code"],
                provider_name=dataset["provider_name"],
                api_link=dataset["api_link"],
            )
            self.session.add(dataset_row)
            self.session.commit()

            hdx_link = resource["hdx_link"]
            hxl_info = hxl.info(hdx_link, InputOptions(encoding="utf-8"))
            is_hxlated = False
            for sheet in hxl_info["sheets"]:
                if sheet["is_hxlated"]:
                    is_hxlated = True
                    break
            resource_row = DBResource(
                code=resource["code"],
                dataset_ref=dataset_row.id,
                hdx_link=resource["hdx_link"],
                filename=resource["filename"],
                format=resource["format"],
                update_date=datetime.strptime(
                    resource["update_date"], "%Y-%m-%dT%H:%M:%S.%f"
                ).date(),
                is_hxl=is_hxlated,
                api_link=resource["api_link"],
            )
            self.session.add(resource_row)
            self.session.commit()
