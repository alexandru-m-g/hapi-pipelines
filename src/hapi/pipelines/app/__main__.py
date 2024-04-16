"""Entry point to start HAPI pipelines"""

import argparse
import logging
from os import getenv

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from hapi.pipelines._version import __version__
from hapi.pipelines.app import (
    load_yamls,
)
from hapi.pipelines.app.pipelines import Pipelines
from hapi.pipelines.utilities.process_config_defaults import add_defaults

setup_logging()
logger = logging.getLogger(__name__)


lookup = "hapi-pipelines"


def parse_args():
    parser = argparse.ArgumentParser(description="HAPI pipelines")
    parser.add_argument("-hk", "--hdx-key", default=None, help="HDX api key")
    parser.add_argument("-ua", "--user-agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument(
        "-hs", "--hdx-site", default=None, help="HDX site to use"
    )
    parser.add_argument(
        "-s",
        "--save",
        default=False,
        action="store_true",
        help="Save data for testing",
    )
    parser.add_argument(
        "-usv",
        "--use-saved",
        default=False,
        action="store_true",
        help="Use saved data",
    )
    return parser.parse_args()


def main(
    save: bool = False,
    use_saved: bool = False,
    **ignore,
) -> None:
    """Run HAPI. Either a database connection string (db_uri) or database
    connection parameters (db_params) can be supplied. If neither is supplied, a local
    SQLite database with filename "hapi.db" is assumed.

    Args:
        save (bool): Whether to save state for testing. Defaults to False.
        use_saved (bool): Whether to use saved state for testing. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    with temp_dir() as temp_folder:
        today = now_utc()
        Read.create_readers(
            temp_folder,
            "saved_data",
            temp_folder,
            save,
            use_saved,
            hdx_auth=configuration.get_api_key(),
            today=today,
        )
        pipelines = Pipelines(
            configuration,
            today,
        )
        pipelines.output()
    logger.info("HAPI pipelines completed!")


if __name__ == "__main__":
    args = parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
        if user_agent is None:
            user_agent = "hapi-pipelines"
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv("HDX_SITE", "prod")
    project_configs = [
        "core.yaml",
    ]
    project_config_dict = load_yamls(project_configs)
    project_config_dict = add_defaults(project_config_dict)
    facade(
        main,
        hdx_key=hdx_key,
        user_agent=user_agent,
        preprefix=preprefix,
        hdx_site=hdx_site,
        project_config_dict=project_config_dict,
        save=args.save,
        use_saved=args.use_saved,
    )
