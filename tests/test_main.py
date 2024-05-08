import logging
from os.path import join

import pytest
from hapi_schema.db_admin1 import DBAdmin1
from hapi_schema.db_admin2 import DBAdmin2
from hapi_schema.db_dataset import DBDataset
from hapi_schema.db_location import DBLocation
from hapi_schema.db_resource import DBResource
from hapi_schema.views import prepare_hapi_views
from hdx.api.configuration import Configuration
from hdx.database import Database
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from sqlalchemy import func, select

from hapi.pipelines.app import load_yamls
from hapi.pipelines.app.__main__ import add_defaults
from hapi.pipelines.app.pipelines import Pipelines

logger = logging.getLogger(__name__)


class TestHAPIPipelines:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        project_configs = [
            "core.yaml",
            "food_security.yaml",
            "humanitarian_needs.yaml",
            "national_risk.yaml",
            "operational_presence.yaml",
            "population.yaml",
        ]
        project_config_dict = load_yamls(project_configs)
        project_config_dict = add_defaults(project_config_dict)
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_dict=project_config_dict,
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def folder(self):
        return join("tests", "fixtures")

    def test_pipelines(self, configuration, folder):
        with ErrorsOnExit() as errors_on_exit:
            with temp_dir(
                "TestHAPIPipelines",
                delete_on_success=True,
                delete_on_failure=False,
            ) as temp_folder:
                db_uri = "postgresql+psycopg://postgres:postgres@localhost:5432/hapitest"
                logger.info(f"Connecting to database {db_uri}")
                with Database(
                    db_uri=db_uri,
                    recreate_schema=True,
                    prepare_fn=prepare_hapi_views,
                ) as database:
                    session = database.get_session()
                    today = parse_date("2023-10-11")
                    Read.create_readers(
                        temp_folder,
                        join(folder, "input"),
                        temp_folder,
                        False,
                        True,
                        today=today,
                    )
                    logger.info("Initialising pipelines")
                    themes_to_run = {
                        "population": ("AFG", "BFA", "MLI", "NGA", "TCD"),
                        "operational_presence": ("AFG", "MLI", "NGA"),
                        "food_security": None,
                        "humanitarian_needs": None,
                        "national_risk": None,
                    }
                    pipelines = Pipelines(
                        configuration,
                        session,
                        today,
                        themes_to_run=themes_to_run,
                        errors_on_exit=errors_on_exit,
                        use_live=False,
                    )
                    logger.info("Running pipelines")
                    pipelines.run()
                    logger.info("Writing to database")
                    pipelines.output()

                    count = session.scalar(
                        select(func.count(DBResource.hdx_id))
                    )
                    assert count == 23
                    count = session.scalar(
                        select(func.count(DBDataset.hdx_id))
                    )
                    assert count == 13
                    count = session.scalar(select(func.count(DBLocation.id)))
                    assert count == 25
                    count = session.scalar(select(func.count(DBAdmin1.id)))
                    assert count == 479
                    count = session.scalar(select(func.count(DBAdmin2.id)))
                    assert count == 5936
                    # TODO
                    # count = session.scalar(select(func.count(DBOrg.id)))
                    # assert count == 497
                    # count = session.scalar(select(func.count(DBOrgType.code)))
                    # assert count == 18
                    # count = session.scalar(select(func.count(DBSector.code)))
                    # assert count == 18
                    # count = session.scalar(select(func.count(DBIpcPhase.code)))
                    # assert count == 7
                    # count = session.scalar(select(func.count(DBIpcType.code)))
                    # assert count == 3
                    # count = session.scalar(select(func.count(DBPopulation.id)))
                    # assert count == 54123
                    # count = session.scalar(
                    #     select(func.count(DBOperationalPresence.id))
                    # )
                    # assert count == 12215
                    # count = session.scalar(
                    #     select(func.count(DBFoodSecurity.id))
                    # )
                    # assert count == 137144
                    # count = session.scalar(
                    #     select(func.count(DBHumanitarianNeeds.id))
                    # )
                    # assert count == 47582
                    # count = session.scalar(
                    #     select(func.count(DBNationalRisk.id))
                    # )
                    # assert count == 25
                    #
                    # org_mapping = pipelines.org._org_lookup
                    # assert org_mapping["Action against Hunger"] == {
                    #     "Action contre la Faim",
                    #     "Action Against Hunger",
                    # }
                    # assert org_mapping["United Nations Children's Fund"] == {
                    #     "Fonds des Nations Unies pour l'Enfance",
                    #     "United Nations Children's Fund",
                    #     "United Nations Children's Emergency Fund",
                    # }
