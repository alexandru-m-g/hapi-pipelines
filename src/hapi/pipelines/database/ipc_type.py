import logging
from typing import Dict

from hapi_schema.db_ipc_type import DBIpcType
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class IpcType(BaseUploader):
    def __init__(
        self, session: Session, ipc_type_descriptions: Dict[int, str]
    ):
        super().__init__(session)
        self._ipc_type_descriptions = ipc_type_descriptions
        self.data = []

    def populate(self):
        logger.info("Populating IPC phase table")
        for (
            ipc_type,
            ipc_type_description,
        ) in self._ipc_type_descriptions.items():
            ipc_type_row = DBIpcType(
                code=ipc_type, description=ipc_type_description
            )
            self._session.add(ipc_type_row)
            self.data.append(ipc_type)
        self._session.commit()
