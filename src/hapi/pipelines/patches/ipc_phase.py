import logging
from typing import Dict

from hapi_schema.db_ipc_phase import DBIpcPhase
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class IpcPhase(BaseUploader):
    def __init__(
        self,
        session: Session,
        ipc_phase_names: Dict[int, str],
        ipc_phase_descriptions: Dict[int, str],
    ):
        super().__init__(session)
        self._ipc_phase_names = ipc_phase_names
        self._ipc_phase_descriptions = ipc_phase_descriptions
        self.data = []

    def generate_hapi_patch(self):
        logger.info("Populating IPC phase table")
        for ipc_phase in self._ipc_phase_names.keys():
            ipc_phase_row = DBIpcPhase(
                code=ipc_phase,
                name=self._ipc_phase_names[ipc_phase],
                description=self._ipc_phase_descriptions[ipc_phase],
            )
            self._session.add(ipc_phase_row)
            self.data.append(ipc_phase)
        self._session.commit()
