from datetime import datetime
from typing import Dict, Optional

from hdx.location.adminlevel import AdminLevel
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from hapi.pipelines.app.locations import Locations
from hapi.pipelines.database.dbdataset import DBDataset
from hapi.pipelines.database.dbresource import DBResource


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
        self.adminone = AdminLevel(configuration["admin1"], admin_level=1)

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
        _create_configurable_scrapers("adminone", adminlevel=self.adminone)

    def run(self):
        self.runner.run()

    def output(self):
        self.locations.populate()
        self.runner.get_results()
        #  Transform and write the results to population schema in db
        #  We need mapping from HXL hashtags in results to gender and age range codes

        # Gets Datasets and Resources
        hapi_metadata = self.runner.get_hapi_metadata()
        for dataset in hapi_metadata:
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

            resource_row = DBResource(
                code=resource["code"],
                dataset_ref=dataset_row.id,
                hdx_link=resource["hdx_link"],
                filename=resource["filename"],
                format=resource["format"],
                update_date=datetime.strptime(
                    resource["update_date"], "%Y-%m-%dT%H:%M:%S.%f"
                ).date(),
                is_hxl=False,  # TODO: needs to be added?
                api_link=resource["api_link"],
            )
            self.session.add(resource_row)
            self.session.commit()
