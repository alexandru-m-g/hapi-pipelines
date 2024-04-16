from abc import ABC, abstractmethod


class BaseUploader(ABC):
    def __init__(self, hapi_repo):
        self._hapi_repo = hapi_repo

    @abstractmethod
    def generate_hapi_patch(self) -> None:
        """
        Generates HAPI patch file. Must be overridden.

        Returns:
            None
        """
