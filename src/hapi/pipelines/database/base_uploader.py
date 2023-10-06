from abc import ABC, abstractmethod

from sqlalchemy.orm import Session


class BaseUploader(ABC):
    def __init__(self, session: Session):
        self._session = session

    @abstractmethod
    def populate(self) -> None:
        """
        Populate database. Must be overridden.

        Returns:
            None
        """
