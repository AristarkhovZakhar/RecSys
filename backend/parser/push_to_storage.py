import argparse
import json
import os
import sys

sys.path.append("/Users/zakhar/Projects/RecSys/backend/")
sys.path.append("/Users/zakhar/Projects/RecSys/backend/storage")

from typing import List
from storage.ya_disk_storage import YaDiskStorage
from configs.config import YAServices

storage = YaDiskStorage("y0_AgAAAAAtTRFlAAnp9QAAAADjS1FmPbAPnfASRgapxZLElKH9_fQ_G3I")

with open('configs/ya.json') as f:
    data = json.load(f)

ya_config = YAServices.from_dict(data)
storage = YaDiskStorage(ya_config.qa_token)


def push_to_yandex(filepathes: List[str]):
    for filepath in filepathes:
        filename = filepath.split('/')[-1]
        storage.upload(filepath, filename)
        os.system(f"rm {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepathes', nargs='+', help='list of filepathes to push')
    args = parser.parse_args()
    push_to_yandex(args.filepathes)
