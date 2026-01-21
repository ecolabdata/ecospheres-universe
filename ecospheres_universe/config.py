from dataclasses import dataclass
from deepmerge import merge_or_raise
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
        dicts = [yaml.safe_load(path.read_text()) for path in paths]
        conf = merge_or_raise.merge(*dicts)
        return dacite.from_dict(Config, conf, config=dacite.Config(cast=[DeployEnv]))
