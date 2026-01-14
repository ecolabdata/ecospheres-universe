from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path

import dacite
import yaml


class DeployEnv(StrEnum):
    DEMO = auto()
    PROD = auto()


@dataclass
class DatagouvConfig:
    url: str
    token: str


@dataclass
class GristConfig:
    url: str
    token: str


@dataclass
class Config:
    env: DeployEnv
    topic: str
    tag: str
    datagouv: DatagouvConfig
    grist: GristConfig
    output_dir: Path = Path("dist")

    @staticmethod
    def from_files(*paths: Path) -> "Config":
        conf = {}
        for path in paths:
            conf.update(yaml.safe_load(path.read_text()))
        return dacite.from_dict(Config, conf, config=dacite.Config(cast=[DeployEnv]))
