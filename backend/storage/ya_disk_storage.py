from yadisk import YaDisk
import sys

sys.path.append("/home/parser/backend/storage")
# from abstract_storage import Storage
from typing import Generator
import os



class YaDiskStorage:
    DIR = "app:/"

    def __init__(self, token: str, dirname: str = "parsed_news"):
        self.disk = YaDisk(token=token)
        self.workdir = self.create_workdir(dirname)
        if not self.disk.check_token():
            raise IndentationError("Invalid token")

    def create_workdir(self, dirname: str) -> str:
        workdir = os.path.join(self.DIR, dirname)
        # self.disk.mkdir(workdir)
        return workdir

    def get_endpoint_path(self, filename: str) -> str:
        return os.path.join(self.workdir, filename)

    def upload(self, source_filepath: str, endpoint_filename: str) -> None:
        with open(source_filepath, 'rb') as f:
            self.disk.upload(
                f,
                self.get_endpoint_path(endpoint_filename)
            )

    def download(self, endpoint_filename: str, source_filepath: str) -> None:
        self.disk.download(
            self.get_endpoint_path(endpoint_filename),
            source_filepath
        )

    def remove(self, endpoint_filename: str) -> None:
        self.disk.remove(self.get_endpoint_path(endpoint_filename))

    def get_files_list(self, sort_datatime: bool = False) -> Generator:
        listdir = self.disk.listdir(self.workdir)
        return sorted(listdir, key=lambda file: file['created'])[::-1] if sort_datatime else listdir
