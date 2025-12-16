from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path

import dacite
import yaml


class DeployEnv(StrEnum):
    DEMO = auto()
    PROD = auto()


@dataclass
class ApiConfig:
    url: str
    token: str


@dataclass
class Config:
    env: DeployEnv
    topic: str
    tag: str
    grist_url: str
    api: ApiConfig

    @staticmethod
    def from_files(*paths: Path) -> "Config":
        conf = {}
        for path in paths:
            conf.update(yaml.safe_load(path.read_text()))
        return dacite.from_dict(Config, conf)
