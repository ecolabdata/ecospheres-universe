import json

from operator import itemgetter
from pathlib import Path
from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from ecospheres_universe.config import ApiConfig, Config, DeployEnv
from ecospheres_universe.datagouv import ElementClass
from ecospheres_universe.feed_universe import feed

from .datagouv_mocks import Dataset, Dataservice, Organization, Topic


def json_load_path(path: Path):
    with open(path, "r") as f:
        return json.load(f)


def mock_organizations_file(organizations: list[Organization]) -> list[dict[str, str]]:
    return sorted(
        [{"id": org.id, "name": org.name, "slug": org.slug, "type": ""} for org in organizations],
        key=itemgetter("name"),
    )


def test_full_run(responses: RequestsMock, tmp_path: Path):
    datagouv_url = "https://www.example.com/datagouv"
    grist_url = "https://www.example.com/grist"
    fake_token = "fake-token"

    organizations = Organization.some(2)

    datasets = Dataset.some_owned(3, organizations)
    dataservices = Dataservice.some_owned(2, organizations)

    existing_universe = Topic(datasets[0], datasets[1], dataservices[0])

    target_universe = (
        existing_universe.clone().add(datasets[2], dataservices[1]).remove(datasets[1])
    )

    conf = Config(
        env=DeployEnv.DEMO,
        topic="test-topic",
        tag="test-tag",
        grist_url="https://www.example.com/grist",
        output_dir=tmp_path,
        api=ApiConfig(url=datagouv_url, token=fake_token),
    )

    # grist.get_organizations()
    _ = responses.get(
        url=grist_url,
        match=[query_param_matcher({"filter": json.dumps({"env": [conf.env.value]}), "limit": 0})],
        json={
            "records": [
                {"fields": {"slug": org.slug, "type": ""}}
                for org in target_universe.organizations()
            ]
        },
    )

    # datagouv.get_organization()
    for org in target_universe.organizations():
        _ = responses.get(
            url=f"{datagouv_url}/api/1/organizations/{org.slug}/",
            json={"id": org.id, "slug": org.slug, "name": org.name},
        )

    # reset=False by default => skip datagouv.delete_all_topic_elements()

    for element_class in ElementClass:
        existing_elements = existing_universe.elements(element_class)
        target_elements = target_universe.elements(element_class)

        # datagouv.get_organization_objects_ids()
        for org in target_universe.organizations():
            _ = responses.get(
                url=f"{datagouv_url}/api/2/{element_class.value}/search/",
                match=[query_param_matcher({"organization": org.id}, strict_match=False)],
                json={
                    "data": [
                        {"id": e.object.id} for e in target_universe.elements(element_class, org)
                    ],
                    "next_page": None,
                },
            )

        # datagouv.get_topic_elements()
        _ = responses.get(
            url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/",
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
            url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/",
            match=[
                header_matcher({"Content-Type": "application/json", "X-API-KEY": fake_token}),
                json_params_matcher(
                    [{"element": {"class": element_class.name, "id": oid}} for oid in additions]
                ),
            ],
        )

        # datagouv.delete_topic_elements()
        removals = [e.id for e in existing_elements if e not in target_elements]
        for eid in removals:
            _ = responses.delete(
                url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/{eid}/",
                match=[header_matcher({"X-API-KEY": fake_token})],
            )

    # datagouv.get_bouquets()
    # TODO: add bouquets
    _ = responses.get(
        url=f"{datagouv_url}/api/2/topics/",
        match=[
            header_matcher({"X-API-KEY": fake_token}),
            query_param_matcher({"tag": conf.tag, "include_private": "yes"}),
        ],
        json={"data": [], "next_page": None},
    )

    feed(conf)

    for element_class in ElementClass:
        actual = json_load_path(
            tmp_path / f"organizations-{element_class.value}-{conf.env.value}.json"
        )
        expected = mock_organizations_file(target_universe.organizations(element_class))
        assert actual == expected

    assert json_load_path(tmp_path / f"organizations-bouquets-{conf.env.value}.json") == []
