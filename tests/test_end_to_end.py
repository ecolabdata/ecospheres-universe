import json

from pathlib import Path
from responses import RequestsMock, matchers
from typing import NamedTuple

from ecospheres_universe.config import ApiConfig, Config, DeployEnv
from ecospheres_universe.datagouv import ElementClass
from ecospheres_universe.feed_universe import feed


class Element(NamedTuple):
    id: str
    object_id: str
    object_class: ElementClass


class Organization(NamedTuple):
    id: str
    slug: str
    name: str
    objects: dict[str, list[str]]


def json_load_path(path: Path):
    with open(path, "r") as f:
        return json.load(f)


def test_full_run(responses: RequestsMock, tmp_path: Path):
    datagouv_url = "https://www.example.com/datagouv"
    grist_url = "https://www.example.com/grist"
    fake_token = "fake-token"

    existing_universe = [
        Element("element-1", "dataset-1", ElementClass.Dataset),
        Element("element-2", "dataset-2", ElementClass.Dataset),
        Element("element-3", "dataservice-1", ElementClass.Dataservice),
    ]

    new_universe = [
        Organization(
            id="org-id-1",
            slug="org-slug-1",
            name="Org 1",
            objects={"datasets": ["dataset-1", "dataset-3"], "dataservices": []},
        ),
        Organization(
            id="org-id-2",
            slug="org-slug-2",
            name="Org 2",
            objects={"datasets": [], "dataservices": ["dataservice-1", "dataservice-2"]},
        ),
    ]

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
        match=[
            matchers.query_param_matcher(
                {"filter": json.dumps({"env": [conf.env.value]}), "limit": 0}
            )
        ],
        json={"records": [{"fields": {"slug": org.slug, "type": ""}} for org in new_universe]},
    )

    # datagouv.get_organization()
    for org in new_universe:
        _ = responses.get(
            url=f"{datagouv_url}/api/1/organizations/{org.slug}/",
            json={"id": org.id, "slug": org.slug, "name": org.name},
        )

    # reset=False by default => skip datagouv.delete_all_topic_elements()

    for element_class in ElementClass:
        existing_elems_for_class = [
            elem for elem in existing_universe if elem.object_class is element_class
        ]
        new_ids_for_class = [
            obj_id for org in new_universe for obj_id in org.objects[element_class.value]
        ]

        # datagouv.get_organization_objects_ids()
        for org in new_universe:
            _ = responses.get(
                url=f"{datagouv_url}/api/2/{element_class.value}/search/",
                match=[matchers.query_param_matcher({"organization": org.id}, strict_match=False)],
                json={
                    "data": [{"id": obj_id} for obj_id in new_ids_for_class],
                    "next_page": None,
                },
            )

        # datagouv.get_topic_elements()
        _ = responses.get(
            url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/",
            match=[
                matchers.query_param_matcher(
                    {"class": element_class.name},
                    strict_match=False,
                )
            ],
            json={
                "data": [
                    {
                        "id": elem.id,
                        "element": {"class": elem.object_class.name, "id": elem.object_id},
                    }
                    for elem in existing_elems_for_class
                ],
                "next_page": None,
            },
        )

        # datagouv.put_topic_elements()
        additions = [
            obj_id
            for obj_id in new_ids_for_class
            if obj_id not in {elem.object_id for elem in existing_elems_for_class}
        ]
        _ = responses.post(
            url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/",
            match=[
                matchers.header_matcher(
                    {"Content-Type": "application/json", "X-API-KEY": fake_token}
                ),
                matchers.json_params_matcher(
                    [
                        {"element": {"class": element_class.name, "id": obj_id}}
                        for obj_id in additions
                    ]
                ),
            ],
        )

        # datagouv.delete_topic_elements()
        removals = [
            elem.id for elem in existing_elems_for_class if elem.object_id not in new_ids_for_class
        ]
        for elem_id in removals:
            _ = responses.delete(
                url=f"{datagouv_url}/api/2/topics/{conf.topic}/elements/{elem_id}/",
                match=[matchers.header_matcher({"X-API-KEY": fake_token})],
            )

    # datagouv.get_bouquets()
    # TODO: add bouquets
    _ = responses.get(
        url=f"{datagouv_url}/api/2/topics/",
        match=[
            matchers.header_matcher({"X-API-KEY": fake_token}),
            matchers.query_param_matcher(
                {
                    "tag": conf.tag,
                    "include_private": "yes",
                }
            ),
        ],
        json={"data": [], "next_page": None},
    )

    feed(conf)

    # FIXME: correct subset of orgs
    assert json_load_path(tmp_path / f"organizations-datasets-{conf.env.value}.json") == [
        {"id": org.id, "name": org.name, "slug": org.slug, "type": ""} for org in new_universe
    ]
    assert json_load_path(tmp_path / f"organizations-dataservices-{conf.env.value}.json") == [
        {"id": org.id, "name": org.name, "slug": org.slug, "type": ""} for org in new_universe
    ]
    assert json_load_path(tmp_path / f"organizations-bouquets-{conf.env.value}.json") == []
