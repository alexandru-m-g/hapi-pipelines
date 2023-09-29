import logging

import hxl
from hapi_schema.db_dataset import DBDataset
from hapi_schema.db_resource import DBResource
from hdx.scraper.runner import Runner
from hxl import InputOptions
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class Metadata:
    def __init__(self, runner: Runner, session: Session):
        self.runner = runner
        self.session = session
        self.dataset_data = {}
        self.resource_data = {}

    def populate(self):
        logger.info("Populating metadata")
        datasets = self.runner.get_hapi_metadata()
        for dataset_id, dataset in datasets.items():
            # First add dataset

            # Make sure dataset hasn't already been added - hapi_metadata
            # contains duplicate datasets since it contains
            # dataset-resource pairs
            if dataset_id in self.dataset_data.keys():
                continue
            dataset_row = DBDataset(
                hdx_id=dataset_id,
                hdx_stub=dataset["hdx_stub"],
                title=dataset["title"],
                provider_code=dataset["provider_code"],
                provider_name=dataset["provider_name"],
            )
            self.session.add(dataset_row)
            self.session.commit()
            self.dataset_data[dataset_id] = dataset_row.id

            resources = dataset["resources"]
            for resource_id, resource in resources.items():
                # Then add the resources
                download_url = resource["download_url"]
                hxl_info = hxl.info(
                    download_url, InputOptions(encoding="utf-8")
                )
                is_hxlated = False
                for sheet in hxl_info["sheets"]:
                    if sheet["is_hxlated"]:
                        is_hxlated = True
                        break
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
                self.resource_data[resource_id] = resource_row.id
