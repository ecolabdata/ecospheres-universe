import json

from collections.abc import Iterable
from operator import itemgetter
from pathlib import Path
from responses import RequestsMock

import pytest

from ecospheres_universe.config import Config, DatagouvConfig, GristConfig
from ecospheres_universe.datagouv import Organization, Topic
from ecospheres_universe.grist import GristEntry
from ecospheres_universe.util import JSONObject, uniquify


from .datagouv_mock import DatagouvMock
from .grist_mock import GristMock


def json_load_path(path: Path) -> JSONObject:
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(
    organizations: Iterable[Organization], categories: dict[str, str | None] | None = None
) -> Iterable[JSONObject]:
    categories = categories or {}
    return sorted(
        [
            {"id": org.id, "name": org.name, "slug": org.slug, "type": categories.get(org.id)}
            for org in organizations
        ],
        key=itemgetter("name"),
    )


def assert_outputs(
    output_dir: Path,
    grist_universe: list[GristEntry],
    upcoming_universe: Topic,
    bouquets: Iterable[Topic] | None = None,
) -> None:
    categories = {
        entry.identifier: entry.category
        for entry in grist_universe
        if entry.object_class is Organization
    }
    for object_class in Topic.object_classes():
        orgs = DatagouvMock.organizations(upcoming_universe.objects_of(object_class))
        assert json_load_path(
            output_dir / f"organizations-{object_class.namespace()}.json"
        ) == mock_organizations_file(orgs, categories)

    orgs = uniquify(org for b in (bouquets or []) if (org := b.organization))
    assert json_load_path(output_dir / "organizations-bouquets.json") == mock_organizations_file(
        orgs
    )


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
