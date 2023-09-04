import logging
from datetime import datetime

import hxl
from hdx.scraper.runner import Runner
from hxl import InputOptions
from sqlalchemy.orm import Session

from hapi.pipelines.database.dbdataset import DBDataset
from hapi.pipelines.database.dbresource import DBResource

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
                hdx_link=dataset["hdx_link"],
                code=dataset["code"],
                title=dataset["title"],
                provider_code=dataset["provider_code"],
                provider_name=dataset["provider_name"],
                api_link=dataset["api_link"],
            )
            self.session.add(dataset_row)
            self.session.commit()

            # Then add the resource
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

            # Add resource to lookup table
            # TODO: this assumes the resource codes are unique
            # TODO: maybe once there is more data we will
            #  want to do direct queries instead?
            self.data[resource["code"]] = resource_row.id
