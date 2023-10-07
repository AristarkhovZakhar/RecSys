import argparse
import json
import os
import sys
#data_dir = '/home/parser'
data_dir = "/opt/airflow/dags"
sys.path.append(f"{data_dir}/")
sys.path.append(f"{data_dir}/configs")
sys.path.append(f"{data_dir}/backend/")
sys.path.append(f"{data_dir}/backend/storage/")

from typing import List
from storage.ya_disk_storage import YaDiskStorage
from configs.config import YAServiceConfig

with open(f'{data_dir}/configs/ya.json') as f:
    data = json.load(f)

ya_config = YAServiceConfig.from_dict(data)
storage = YaDiskStorage(ya_config.qa_token)


def push_to_yandex(filepathes: List[str]):
    for filepath in filepathes:
        filename = filepath.split('/')[-1]
        #storage.upload(filepath, filename)
        #os.system(f"rm {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepathes', nargs='+', help='list of filepathes to push')
    args = parser.parse_args()
    push_to_yandex(args.filepathes)
