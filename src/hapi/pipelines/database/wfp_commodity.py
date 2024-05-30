"""Populate the WFP commodity table."""

from logging import getLogger
from typing import Dict, Optional

from hapi_schema.db_wfp_commodity import DBWFPCommodity
from hapi_schema.utils.enums import CommodityCategory
from hdx.scraper.utilities.reader import Read
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = getLogger(__name__)


class WFPCommodity(BaseUploader):
    def __init__(
        self,
        session: Session,
        datasetinfo: Dict[str, str],
    ):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}

    def populate(self):
        logger.info("Populating WFP commodity table")
        reader = Read.get_reader("hdx")
        headers, iterator = reader.read(datasetinfo=self._datasetinfo)
        next(iterator)  # ignore HXL hashtags
        for commodity in iterator:
            code = commodity["commodity_id"]
            category = CommodityCategory(commodity["category"])
            name = commodity["commodity"]

            commodity_row = DBWFPCommodity(
                code=code, category=category, name=name
            )
            self.data[name] = code
            self.data[code] = name
            self._session.add(commodity_row)
        self._session.commit()

    def get_commodity_code(self, commodity: str) -> Optional[str]:
        return self.data.get(commodity)
