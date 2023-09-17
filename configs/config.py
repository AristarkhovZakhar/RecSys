from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Dict, Optional


@dataclass_json
@dataclass
class API:
    api_id: int
    api_hash: str
    username: str
    user_id: int
    send_to: str
    master: str


@dataclass_json
@dataclass
class Channels:
    channels: Optional


@dataclass_json
@dataclass
class YAServices:
    qa_token: str


@dataclass_json
@dataclass
class MainConfig:
    document_endpoint: str
    hf_access_token: str
    yandex_qa_token: str

