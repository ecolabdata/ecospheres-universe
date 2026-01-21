from dataclasses import dataclass
from deepmerge import always_merger
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
    table: str
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
        dicts = [yaml.safe_load(path.read_text()) for path in paths]
        conf = always_merger.merge(*dicts)
        return dacite.from_dict(Config, conf, config=dacite.Config(cast=[DeployEnv]))
