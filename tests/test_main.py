import logging
from os import remove
from os.path import join

import pytest
from hapi_schema.db_admin1 import DBAdmin1
from hapi_schema.db_admin2 import DBAdmin2
from hapi_schema.db_age_range import DBAgeRange
from hapi_schema.db_dataset import DBDataset
from hapi_schema.db_food_security import DBFoodSecurity
from hapi_schema.db_gender import DBGender
from hapi_schema.db_ipc_phase import DBIpcPhase
from hapi_schema.db_ipc_type import DBIpcType
from hapi_schema.db_location import DBLocation
from hapi_schema.db_operational_presence import DBOperationalPresence
from hapi_schema.db_org import DBOrg
from hapi_schema.db_org_type import DBOrgType
from hapi_schema.db_population import DBPopulation
from hapi_schema.db_resource import DBResource
from hapi_schema.db_sector import DBSector
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
                dbpath = join(temp_folder, "test_hapi.db")
                try:
                    remove(dbpath)
                except OSError:
                    pass
                logger.info(f"Creating database {dbpath}")
                with Database(database=dbpath, dialect="sqlite") as session:
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
                    pipelines = Pipelines(
                        configuration,
                        session,
                        today,
                        errors_on_exit=errors_on_exit,
                        use_live=False,
                    )
                    logger.info("Running pipelines")
                    pipelines.run()
                    logger.info("Writing to database")
                    pipelines.output()

                    count = session.scalar(select(func.count(DBLocation.id)))
                    assert count == 5
                    count = session.scalar(select(func.count(DBAdmin1.id)))
                    assert count == 122
                    count = session.scalar(select(func.count(DBAdmin2.id)))
                    assert count == 1465
                    count = session.scalar(select(func.count(DBDataset.id)))
                    assert count == 7
                    count = session.scalar(select(func.count(DBResource.id)))
                    assert count == 12
                    count = session.scalar(select(func.count(DBOrgType.code)))
                    assert count == 14
                    count = session.scalar(select(func.count(DBSector.code)))
                    assert count == 15
                    count = session.scalar(select(func.count(DBGender.code)))
                    assert count == 3
                    count = session.scalar(select(func.count(DBAgeRange.code)))
                    assert count == 17
                    count = session.scalar(select(func.count(DBPopulation.id)))
                    assert count == 45861
                    count = session.scalar(select(func.count(DBOrg.id)))
                    assert count == 539
                    count = session.scalar(
                        select(func.count(DBOperationalPresence.id))
                    )
                    assert count == 12215
                    count = session.scalar(select(func.count(DBIpcPhase.code)))
                    assert count == 7
                    count = session.scalar(select(func.count(DBIpcType.code)))
                    assert count == 3
                    count = session.scalar(
                        select(func.count(DBFoodSecurity.id))
                    )
                    assert count == 103264
