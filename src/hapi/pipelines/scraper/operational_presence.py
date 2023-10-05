"""Functions specific to the operational presence theme."""
from logging import getLogger
from typing import Dict

from hdx.scraper.base_scraper import BaseScraper

logger = getLogger(__name__)


class OperationalPresenceScraper(BaseScraper):
    def __init__(
        self,
        country_code: str,
        datasetinfo: Dict,
    ):
        super().__init__(
            f"operational_presence_{country_code}",
            datasetinfo,
            dict(),
        )
        self._scraped_data = {}

    # TODO: make this handle all countries once metadata issue is solved
    def run(self):
        reader = self.get_reader()
        self._scraped_data = []
        _, iterator = reader.read(self.datasetinfo)
        for row in iterator:
            newrow = {}
            for input_header, output_header in zip(
                self.datasetinfo["input"],
                self.datasetinfo["output"],
            ):
                newrow[output_header] = row[input_header]
            # TODO: This is necessary because if the data is at the admin3
            #  level, there will be several duplicates. We should handle
            #  this better, e.g. define a level for each 3W file if possible.
            if newrow in self._scraped_data:
                continue
            self._scraped_data.append(newrow)

    def add_sources(self):
        return
