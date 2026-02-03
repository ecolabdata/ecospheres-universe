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


def json_load_path(path: Path) -> JSONObject:
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(organizations: Iterable[Organization]) -> Iterable[JSONObject]:
    return sorted(
        [{"id": org.id, "name": org.name, "slug": org.slug} for org in organizations],
        key=itemgetter("name"),
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


def mock_feed(
    datagouv: DatagouvMock,
    grist: GristMock,
    existing_universe: Topic,
    upcoming_universe: Topic,
    bouquets: Iterable[Topic] | None = None,
):
    # grist.get_entries()
    grist.mock_get_entries(upcoming_universe)

    # datagouv.get_organization()
    datagouv.mock_get_organization(upcoming_universe)

    # datagouv.delete_all_topic_elements()
    # TODO: support reset=True

    for object_class in Topic.object_classes():
        upcoming_elements = upcoming_universe.elements(object_class)
        existing_elements = existing_universe.elements(object_class)

        # datagouv.get_organization_objects_ids()
        datagouv.mock_get_organization_object_ids(upcoming_universe, object_class)

        # datagouv.get_topic_elements()
        datagouv.mock_topic_elements(existing_universe, object_class)

        existing_object_ids = {e.object.id for e in existing_elements}
        upcoming_object_ids = {e.object.id for e in upcoming_elements}

        # datagouv.put_topic_elements()
        additions = sorted(
            {e.object.id for e in upcoming_elements if e.object.id not in existing_object_ids}
        )
        if additions:
            datagouv.mock_put_topic_elements(additions, object_class)

        # datagouv.delete_topic_elements()
        removals = sorted(
            {e.id for e in existing_elements if e.object.id not in upcoming_object_ids}
        )
        datagouv.mock_delete_topic_elements(removals)

    # datagouv.get_bouquets()
    datagouv.mock_get_bouquets(bouquets or [])


def assert_outputs(
    datagouv: DatagouvMock,
    upcoming_universe: Topic,
    bouquets: Iterable[Topic] | None = None,
) -> None:
    for object_class in Topic.object_classes():
        orgs = datagouv.get_organizations(upcoming_universe.objects(object_class))
        assert json_load_path(
            datagouv.config.output_dir / f"organizations-{object_class.namespace()}.json"
        ) == mock_organizations_file(orgs)

    orgs = uniquify(org for b in (bouquets or []) if (org := b.organization))
    assert json_load_path(
        datagouv.config.output_dir / "organizations-bouquets.json"
    ) == mock_organizations_file(orgs)
