import json

from collections.abc import Iterable
from operator import itemgetter
from pathlib import Path
from typing import Any, Callable

import pytest

from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from ecospheres_universe.config import ApiConfig, Config, DeployEnv
from ecospheres_universe.datagouv import ElementClass
from ecospheres_universe.feed_universe import feed

from .datagouv_mocks import Organization, Topic


def json_load_path(path: Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(organizations: Iterable[Organization]) -> list[dict[str, Any]]:
    return sorted(
        [
            {"id": org.id, "name": org.name, "slug": org.slug, "type": org.type}
            for org in organizations
        ],
        key=itemgetter("name"),
    )


@pytest.fixture
def feed_config(tmp_path: Path) -> Config:
    return Config(
        env=DeployEnv.DEMO,
        topic="test-topic",
        tag="test-tag",
        grist_url="https://www.example.com/grist",
        output_dir=tmp_path,
        api=ApiConfig(
            url="https://www.example.com/datagouv",
            token="fake-token",
        ),
    )


@pytest.fixture
def run_mock_feed(responses: RequestsMock) -> Callable:
    def _run_mock_feed(
        config: Config,
        existing_universe: Topic,
        target_universe: Topic,
        bouquets: list[Topic] | None = None,
    ):
        bouquets: list[Topic] = bouquets or []

        # grist.get_organizations()
        _ = responses.get(
            url=config.grist_url,
            match=[
                query_param_matcher({"filter": json.dumps({"env": [config.env.value]}), "limit": 0})
            ],
            json={
                "records": [
                    {"fields": {"slug": org.slug, "type": org.type}}
                    for org in target_universe.organizations()
                ]
            },
        )

        # datagouv.get_organization()
        for org in target_universe.organizations():
            _ = responses.get(
                url=f"{config.api.url}/api/1/organizations/{org.slug}/",
                json={"id": org.id, "slug": org.slug, "name": org.name},
            )

        # reset=False by default => skip datagouv.delete_all_topic_elements()

        for element_class in ElementClass:
            existing_elements = existing_universe.elements(element_class)
            target_elements = target_universe.elements(element_class)

            # datagouv.get_organization_objects_ids()
            for org in target_universe.organizations():
                _ = responses.get(
                    url=f"{config.api.url}/api/2/{element_class.value}/search/",
                    match=[query_param_matcher({"organization": org.id}, strict_match=False)],
                    json={
                        "data": [
                            {"id": e.object.id}
                            for e in target_universe.elements(element_class, org)
                        ],
                        "next_page": None,
                    },
                )

            # datagouv.get_topic_elements()
            _ = responses.get(
                url=f"{config.api.url}/api/2/topics/{config.topic}/elements/",
                match=[query_param_matcher({"class": element_class.name}, strict_match=False)],
                json={
                    "data": [
                        {"id": e.id, "element": {"class": element_class.name, "id": e.object.id}}
                        for e in existing_elements
                    ],
                    "next_page": None,
                },
            )

            # datagouv.put_topic_elements()
            additions = [e.object.id for e in target_elements if e not in existing_elements]
            _ = responses.post(
                url=f"{config.api.url}/api/2/topics/{config.topic}/elements/",
                match=[
                    header_matcher(
                        {"Content-Type": "application/json", "X-API-KEY": config.api.token}
                    ),
                    json_params_matcher(
                        [{"element": {"class": element_class.name, "id": oid}} for oid in additions]
                    ),
                ],
            )

            # datagouv.delete_topic_elements()
            removals = [e.id for e in existing_elements if e not in target_elements]
            for eid in removals:
                _ = responses.delete(
                    url=f"{config.api.url}/api/2/topics/{config.topic}/elements/{eid}/",
                    match=[header_matcher({"X-API-KEY": config.api.token})],
                )

        # datagouv.get_bouquets()
        _ = responses.get(
            url=f"{config.api.url}/api/2/topics/",
            match=[
                header_matcher({"X-API-KEY": config.api.token}),
                query_param_matcher({"tag": config.tag, "include_private": "yes"}),
            ],
            json={"data": [b.as_dict() for b in bouquets], "next_page": None},
        )

        feed(config)

        for element_class in ElementClass:
            assert json_load_path(
                config.output_dir / f"organizations-{element_class.value}-{config.env.value}.json"
            ) == mock_organizations_file(target_universe.organizations(element_class))

        assert json_load_path(
            config.output_dir / f"organizations-bouquets-{config.env.value}.json"
        ) == mock_organizations_file({b.organization for b in bouquets})

    return _run_mock_feed
