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
class LabelerTags:
    tags: Optional


@dataclass_json
@dataclass
class HFConfig:
    access_token: str
