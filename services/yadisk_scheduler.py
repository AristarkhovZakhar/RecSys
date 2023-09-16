import sys
import os

from typing import List

sys.path.append('../storage')

from abc import ABC
from backend.storage.ya_disk_storage import YaDiskStorage


class Scheduler(ABC):
    def listen(self):
        ...

    def run_push_service(self):
        ...


class YaDiskScheduler(Scheduler):
    TMP_STORAGE_TO_READ = '../storage_to_read/file'

    def __init__(self, storage: YaDiskStorage, files_to_push: int = 50):
        self.storage = storage
        self.files_to_push = files_to_push

    def listen(self):
        return list(self.storage.get_files_list(sort_datatime=True))[:self.files_to_push]

    def run_push_service(self, **kwargs):
        print(kwargs)
        ti = kwargs['ti']
        while len(files := self.listen()) != self.files_to_push:
            pass

        texts = []
        for file in files:
            self.storage.download(file['path'].split('/')[-1], self.TMP_STORAGE_TO_READ)
            with open(self.TMP_STORAGE_TO_READ) as f:
                texts.append(f.readlines())
        os.remove(self.TMP_STORAGE_TO_READ)

        ti.xcom_push(key='texts for webservice', value=texts)
        return texts
