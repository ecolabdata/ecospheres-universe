from dataclasses import dataclass
from deepmerge import always_merger
from pathlib import Path

import dacite
import yaml


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
    topic: str
    tag: str
    datagouv: DatagouvConfig
    grist: GristConfig
    output_dir: Path = Path("dist")

    @staticmethod
    def from_files(*paths: Path) -> "Config":
        assert len(paths) > 0
        dicts = [yaml.safe_load(path.read_text()) for path in paths]
        conf = dicts[0] if len(dicts) == 1 else always_merger.merge(*dicts)
        return dacite.from_dict(Config, conf, config=dacite.Config(cast=[Path]))
