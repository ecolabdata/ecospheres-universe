import json

from collections.abc import Iterable
from operator import itemgetter
from pathlib import Path
from responses import RequestsMock

import pytest

from ecospheres_universe.config import Config, DatagouvConfig, GristConfig
from ecospheres_universe.datagouv import Organization, Topic
from ecospheres_universe.util import JSONObject, uniquify


from .datagouv_mock import DatagouvMock
from .grist_mock import GristMock


@pytest.fixture
def config(tmp_path: Path) -> Config:
    return Config(
        topic="test-topic",
        tag="test-tag",
        datagouv=DatagouvConfig(url="https://www.example.com/datagouv", token="datagouv-token"),
        grist=GristConfig(url="https://www.example.com/grist", table="test", token="grist-token"),
        output_dir=tmp_path,
    )


@pytest.fixture
def datagouv(responses: RequestsMock, config: Config) -> DatagouvMock:
    return DatagouvMock(responses, config)


@pytest.fixture
def grist(responses: RequestsMock, config: Config) -> GristMock:
    return GristMock(responses, config)


def json_load_path(path: Path) -> JSONObject:
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(organizations: Iterable[Organization]) -> Iterable[JSONObject]:
    return sorted(
        [{"id": org.id, "name": org.name, "slug": org.slug} for org in organizations],
        key=itemgetter("name"),
    )


def assert_outputs(
    datagouv: DatagouvMock,
    upcoming_universe: Topic,
    bouquets: Iterable[Topic] | None = None,
) -> None:
    for object_class in Topic.object_classes():
        orgs = datagouv.organizations(upcoming_universe.objects_of(object_class))
        assert json_load_path(
            datagouv.config.output_dir / f"organizations-{object_class.namespace()}.json"
        ) == mock_organizations_file(orgs)

    orgs = uniquify(org for b in (bouquets or []) if (org := b.organization))
    assert json_load_path(
        datagouv.config.output_dir / "organizations-bouquets.json"
    ) == mock_organizations_file(orgs)
