from datetime import datetime

from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources

from hapi.pipelines.database import dbdataset, dbresource


class Pipelines:
    def __init__(
        self,
        configuration,
        session,
        today,
        scrapers_to_run=None,
        errors_on_exit=None,
        use_live=True,
        fallbacks_root=None,
    ):
        self.configuration = configuration
        self.session = session
        self.use_live = use_live
        Country.countriesdata(
            use_live=use_live,
            country_name_overrides=configuration["country_name_overrides"],
            country_name_mappings=configuration["country_name_mappings"],
        )
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
        self.runner.get_results()
        #  Transform and write the results to population schema in db
        #  We need mapping from HXL hashtags in results to gender and age range codes

        # Gets Datasets and Resources
        hapi_metadata = self.runner.get_hapi_metadata()
        dataset = hapi_metadata[0]
        resource = dataset["resource"]

        dataset_row = dbdataset.Dataset(
            hdx_link=dataset["hdx_link"],
            code=dataset["code"],
            title=dataset["title"],
            provider_code=dataset["provider_code"],
            provider_name=dataset["provider_name"],
            api_link=dataset["api_link"],
        )
        self.session.add(dataset_row)
        self.session.commit()

        resource_row = dbresource.Resource(
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
