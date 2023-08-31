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

from hapi.pipelines.app import population
from hapi.pipelines.database.dbadmin2 import DBAdmin2
from hapi.pipelines.database.dbdataset import DBDataset
from hapi.pipelines.database.dbpopulation import DBPopulation
from hapi.pipelines.database.dbresource import DBResource
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
        # Populate the locations table
        self.locations.populate()
        # Gets metadata and populate dataset and resource table
        # TODO: maybe make this its own method? Its' a bit long
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
        # Get the population results and populate population table
        # TODO: what happens to the structure when other themes are included?
        #  Below it's written assuming admin1 population only,
        #  will need to be changed
        # TODO: How should the gender and age tables be populated?
        #  For now this is taken care of in the schema itself.
        #  For age in particular, should we populated it using the
        #  data or pre-define all values here in the codebase?
        population_results = self.runner.get_results()["adminone"]
        for hxl_column, values in zip(
            population_results["headers"][1], population_results["values"]
        ):
            mappings = population.hxl_mapping[hxl_column]
            for admin_code, value in values.items():
                # TODO: turn this into a lookup table?
                # TODO: this actually needs to provide the lookup to
                #  the admin2 table. If we're dealing with admin1
                #  then how do we get that?
                # Query the table and retrieve the id where code is "ABC"
                admin_code = (
                    "FOO-001-XXX"  # Remove this once table is populated
                )
                admin_row = (
                    self.session.query(DBAdmin2)
                    .filter(DBAdmin2.code == admin_code)
                    .first()
                )
                if not admin_row:
                    raise ValueError(f"Pcode {admin_code} missing")
                population_row = DBPopulation(
                    # TODO: Some open questions so filling with
                    #  fake data for now
                    # TODO: Somehow need to connect to resource
                    resource_ref=resource_row.id,
                    admin2_ref=admin_row.id,
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
