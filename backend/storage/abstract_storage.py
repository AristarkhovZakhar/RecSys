from abc import ABC, abstractmethod
from typing import List


class Storage(ABC):

    @abstractmethod
    def create_workdir(cls, dirname: str) -> str:
        ...

    @abstractmethod
    def get_endpoint_path(self, filename: str) -> str:
        ...

    @abstractmethod
    def upload(self, source_filepath: str, endpoint_filename: str) -> None:
        ...

    @abstractmethod
    def download(self, endpoint_filename: str, source_filepath: str) -> None:
        ...

    @abstractmethod
    def remove(self, endpoint_filename: str) -> None:
        ...

    @abstractmethod
    def get_files_list(self) -> List[str]:
        ...
