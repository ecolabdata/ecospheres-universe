import json

from collections.abc import Iterable
from operator import itemgetter
from pathlib import Path
from typing import Any, Callable

import pytest

from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from ecospheres_universe.config import Config, DatagouvConfig, GristConfig
from ecospheres_universe.datagouv import ElementClass, INACTIVE_OBJECT_MARKERS
from ecospheres_universe.feed_universe import feed

from .datagouv_mocks import Organization, Topic


def json_load_path(path: Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(organizations: Iterable[Organization]) -> list[dict[str, Any]]:
    return sorted(
        [
            {"id": org.id, "name": org.name, "slug": org.slug, "type": org.category}
            for org in organizations
        ],
        key=itemgetter("name"),
    )


@pytest.fixture
def feed_config(tmp_path: Path) -> Config:
    return Config(
        topic="test-topic",
        tag="test-tag",
        datagouv=DatagouvConfig(url="https://www.example.com/datagouv", token="datagouv-token"),
        grist=GristConfig(url="https://www.example.com/grist", table="test", token="grist-token"),
        output_dir=tmp_path,
    )


@pytest.fixture
def mock_feed_and_assert(responses: RequestsMock) -> Callable:
    def _run_mock_feed(
        config: Config,
        existing_universe: Topic,
        upcoming_universe: Topic,
        bouquets: list[Topic] | None = None,
    ):
        bouquets = bouquets or list[Topic]()

        # grist.get_entries()
        _ = responses.get(
            url=f"{config.grist.url}/tables/{config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {
                        "fields": {
                            "Type": org.type,
                            "Identifiant": org.slug,
                            "Categorie": org.category,
                        }
                    }
                    for org in upcoming_universe.organizations()
                ]
            },
        )

        # datagouv.get_organization()
        for org in upcoming_universe.organizations():
            _ = responses.get(
                url=f"{config.datagouv.url}/api/1/organizations/{org.slug}/",
                json={"id": org.id, "slug": org.slug, "name": org.name},
            )

        # datagouv.delete_all_topic_elements()
        # TODO: support reset=True

        for element_class in ElementClass:
            upcoming_elements = upcoming_universe.elements(element_class)
            existing_elements = existing_universe.elements(element_class)

            # datagouv.get_organization_objects_ids()
            for org in upcoming_universe.organizations():
                _ = responses.get(
                    url=f"{config.datagouv.url}/api/2/{element_class.value}/search/",
                    match=[
                        header_matcher(
                            {
                                "X-Fields": f"data{{id,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                            }
                        ),
                        query_param_matcher({"organization": org.id}, strict_match=False),
                    ],
                    json={
                        "data": [
                            {"id": e.object.id}
                            for e in upcoming_elements
                            if e.object.organization == org
                        ],
                        "next_page": None,
                    },
                )

            # datagouv.get_topic_elements()
            _ = responses.get(
                url=f"{config.datagouv.url}/api/2/topics/{config.topic}/elements/",
                match=[
                    header_matcher({"X-Fields": "data{id,element{id}},next_page"}),
                    query_param_matcher({"class": element_class.name}, strict_match=False),
                ],
                json={
                    "data": [
                        {"id": e.id, "element": {"class": element_class.name, "id": e.object.id}}
                        for e in existing_elements
                    ],
                    "next_page": None,
                },
            )

            existing_object_ids = {e.object.id for e in existing_elements}
            upcoming_object_ids = {e.object.id for e in upcoming_elements}

            # datagouv.put_topic_elements()
            additions = sorted(
                {e.object.id for e in upcoming_elements if e.object.id not in existing_object_ids}
            )
            if additions:
                # TODO: support batching
                _ = responses.post(
                    url=f"{config.datagouv.url}/api/2/topics/{config.topic}/elements/",
                    match=[
                        header_matcher(
                            {"Content-Type": "application/json", "X-API-KEY": config.datagouv.token}
                        ),
                        json_params_matcher(
                            [
                                {"element": {"class": element_class.name, "id": oid}}
                                for oid in additions
                            ]
                        ),
                    ],
                )

            # datagouv.delete_topic_elements()
            removals = sorted(
                {e.id for e in existing_elements if e.object.id not in upcoming_object_ids}
            )
            for eid in removals:
                _ = responses.delete(
                    url=f"{config.datagouv.url}/api/2/topics/{config.topic}/elements/{eid}/",
                    match=[header_matcher({"X-API-KEY": config.datagouv.token})],
                )

        # datagouv.get_bouquets()
        _ = responses.get(
            url=f"{config.datagouv.url}/api/2/topics/",
            match=[
                header_matcher(
                    {
                        "X-API-KEY": config.datagouv.token,
                        "X-Fields": "data{id,name,organization{id,name,slug}},next_page",
                    }
                ),
                query_param_matcher({"tag": config.tag, "include_private": "yes"}),
            ],
            json={"data": [b.as_json() for b in bouquets], "next_page": None},
        )
        feed(config)

        for element_class in ElementClass:
            assert json_load_path(
                config.output_dir / f"organizations-{element_class.value}.json"
            ) == mock_organizations_file(upcoming_universe.organizations(element_class))

        assert json_load_path(
            config.output_dir / "organizations-bouquets.json"
        ) == mock_organizations_file({b.organization for b in bouquets})

    return _run_mock_feed
