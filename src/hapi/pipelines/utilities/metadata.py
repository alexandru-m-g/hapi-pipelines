import logging

import hxl
from hdx.scraper.runner import Runner
from hxl import InputOptions
from sqlalchemy.orm import Session

from hapi.pipelines.database.db_dataset import DBDataset
from hapi.pipelines.database.db_resource import DBResource

logger = logging.getLogger(__name__)


class Metadata:
    def __init__(self, runner: Runner, session: Session):
        self.runner = runner
        self.session = session
        self.data = {}

    def populate(self):
        logger.info("Populating metadata")
        hapi_metadata = self.runner.get_hapi_metadata()
        # TODO: Not sure if this will work once we have multiple
        #  resources per dataset
        for dataset in hapi_metadata:
            # First add dataset
            resource = dataset["resource"]
            dataset_row = DBDataset(
                hdx_id=dataset["hdx_id"],
                hdx_stub=dataset["hdx_stub"],
                title=dataset["title"],
                provider_code=dataset["provider_code"],
                provider_name=dataset["provider_name"],
            )
            self.session.add(dataset_row)
            self.session.commit()

            # Then add the resource
            download_url = resource["download_url"]
            hxl_info = hxl.info(download_url, InputOptions(encoding="utf-8"))
            is_hxlated = False
            for sheet in hxl_info["sheets"]:
                if sheet["is_hxlated"]:
                    is_hxlated = True
                    break
            resource_id = resource["hdx_id"]
            resource_row = DBResource(
                hdx_id=resource_id,
                dataset_ref=dataset_row.id,
                filename=resource["filename"],
                format=resource["format"],
                update_date=resource["update_date"],
                is_hxl=is_hxlated,
                download_url=download_url,
            )
            self.session.add(resource_row)
            self.session.commit()

            # Add resource to lookup table
            self.data[resource_id] = resource_row.id
