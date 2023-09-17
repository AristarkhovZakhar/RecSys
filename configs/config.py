from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class APIConfig:
    api_id: int
    api_hash: str
    username: str
    user_id: int
    send_to: str
    master: str


@dataclass_json
@dataclass
class ChannelsConfig:
    channels: Optional


@dataclass_json
@dataclass
class YAServiceConfig:
    qa_token: str


@dataclass_json
@dataclass
class DocumentServiceConfig:
    endpoint: str


@dataclass_json
@dataclass
class LabelerConfig:
    hf_token: str
    labels: Optional


@dataclass_json
@dataclass
class HFConfig:
    access_token: str
