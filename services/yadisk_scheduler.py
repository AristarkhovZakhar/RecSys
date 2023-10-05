import sys
import os

from typing import List
from abc import ABC
from backend.storage.ya_disk_storage import YaDiskStorage

sys.path.append('../storage')


class Scheduler(ABC):
    def listen(self):
        ...

    def run_push_service(self):
        ...


class YaDiskScheduler(Scheduler):
    def __init__(self, path_to_storage: str, n_files_to_push: int = 2):
        self.path_to_storage = path_to_storage
        self.n_files_to_push = n_files_to_push

    def listen(self):
        return sorted(os.listdir(self.path_to_storage), key=lambda x:int(x.split('.')[0]))[:self.n_files_to_push]

    def _download_files(self, files):
        texts = []
        filepathes = []
        for file in files:
            filepath = os.path.join(self.path_to_storage, file)
            filepathes.append(filepath)
            with open(filepath) as f:
                texts.append(' '.join(f.readlines()))
        return filepathes, texts

    def run_push_service(self, **kwargs):
        ti = kwargs['ti']
        while len(files := self.listen()) != self.n_files_to_push:
            pass
        filepathes, texts = self._download_files(files)
        print(texts)
        ti.xcom_push(key='texts for webservice', value=texts)
        ti.xcom_push(key='filepathes to remove', value=filepathes)
        return texts
