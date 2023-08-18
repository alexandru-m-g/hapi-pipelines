from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources

from hapi.pipelines.database.dbresource import DBResource


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
        self.runner.get_results()
        #  Transform and write the results to population schema in db
        #  We need mapping from HXL hashtags in results to gender and age range codes
        #  Probably should expand scraper framework to pull out all the resource stuff below (as well as dataset info)...
        #  Need to fix fake dataset_ref below once we have a dataset table
        dbresource = DBResource(
            code="e8f7fb08-af9c-4bdf-8a49-a54c56a4a1b0",
            dataset_ref=0,
            hdx_link="https://data.humdata.org/dataset/8520e386-9263-48c9-b1bf-b2349e019fbb/resource/e8f7fb08-af9c-4bdf-8a49-a54c56a4a1b0/download/col_admpop_adm1_2023.csv",
            filename="col_admpop_adm1_2023.csv",
            mime_type="csv",
            last_modified=self.runner.today,
            is_hxl=False,
            api_link="https://data.humdata.org/api/3/action/resource_show?id=e8f7fb08-af9c-4bdf-8a49-a54c56a4a1b0",
        )
        self.session.add(dbresource)

        self.session.commit()
