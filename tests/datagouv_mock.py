from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import cast

from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import (
    INACTIVE_OBJECT_MARKERS,
    DatagouvObject,
    Dataservice,
    Dataset,
    Organization,
    Owned,
    Topic,
    TopicElement,
    TopicObject,
)
from ecospheres_universe.util import uniquify


@dataclass
class MockObject:
    object: DatagouvObject
    children: list[TopicObject] = field(default_factory=list)


class DatagouvMock:
    responses: RequestsMock
    config: Config
    objects: dict[str, MockObject]  # careful about name collisions?

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config
        self.objects = {}

    def dataservice(self, id: str) -> Dataservice | None:
        if obj := self.objects.get(id):
            return cast(Dataservice, obj.object)

    def dataservices(self, *ids: str) -> list[Dataservice]:
        return [dataservice for id in ids if (dataservice := self.dataservice(id))]

    def make_dataservice(
        self,
        id: str,
        organization: str | None = None,
        tags: list[str] | None = None,
        topics: list[str] | None = None,
    ) -> Dataservice:
        org = self.organization(organization) if organization else None
        dataservice = Dataservice(id=id, slug=id, title=id, organization=org)  # TODO: pretty name
        self.objects[id] = MockObject(dataservice, [dataservice])  # FIXME: children...
        if organization:
            self.objects[organization].children.append(dataservice)
        return dataservice

    def dataset(self, id: str) -> Dataset | None:
        if obj := self.objects.get(id):
            return cast(Dataset, obj.object)

    def datasets(self, *ids: str) -> list[Dataset]:
        return [dataset for id in ids if (dataset := self.dataset(id))]

    def make_dataset(
        self,
        id: str,
        organization: str | None = None,
        tags: list[str] | None = None,
        topics: list[str] | None = None,
    ) -> Dataset:
        org = self.organization(organization) if organization else None
        dataset = Dataset(id=id, slug=id, title=id, organization=org)  # TODO: pretty name
        self.objects[id] = MockObject(dataset, [dataset])  # FIXME: children
        if organization:
            self.objects[organization].children.append(dataset)
        return dataset

    def organization(self, id: str) -> Organization | None:
        if obj := self.objects.get(id):
            return cast(Organization, obj.object)

    def organizations(self, *ids: str) -> list[Organization]:
        return [organization for id in ids if (organization := self.organization(id))]

    def organization_objects(self, id: str) -> list[Owned]:
        return [cast(Owned, obj) for obj in self.objects[id].children]

    def make_organization(self, id: str) -> Organization:
        # TODO: check if already exists?
        org = Organization(id=id, slug=id, name=id)  # TODO: pretty name
        self.objects[id] = MockObject(org)
        return org

    def topic(self, id: str) -> Topic | None:
        if obj := self.objects.get(id):
            return cast(Topic, obj.object)

    def topics(self, *ids: str) -> list[Topic]:
        return [topic for id in ids if (topic := self.topic(id))]

    def topic_objects(self, id: str) -> list[TopicObject]:
        return self.objects[id].children

    def make_topic(self, id: str, organization: str | None = None) -> Topic:
        org = self.organization(organization) if organization else None
        topic = Topic(id=id, slug=id, name=id, organization=org)  # TODO: pretty name
        return topic

    def make_universe(self, id: str, objects: list[TopicObject]) -> Topic:
        return Topic(
            id=id,
            slug=id,
            name=id,  # TODO: pretty name
            elements=[TopicElement(f"element-{obj.id}", obj) for obj in objects],
        )

    def mock_get_organization(self, universe: Topic) -> None:
        orgs = uniquify(org for obj in universe.objects if (org := obj.organization))
        for org in orgs:
            _ = self.responses.get(
                url=f"{self.config.datagouv.url}/api/1/organizations/{org.slug}/",
                json={"id": org.id, "slug": org.slug, "name": org.name},
            )

    def mock_get_organization_object_ids(
        self, universe: Topic, object_class: type[TopicObject]
    ) -> None:
        # warning: orgs must be over ALL orgs, not just those matching type
        # requests have to be mocked even if they don't return anything
        orgs = uniquify(org for obj in universe.objects if (org := obj.organization))
        # TODO: better: group by org => orgs = group keys + "data" = group values
        objects = universe.objects_of(object_class)
        for org in orgs:
            _ = self.responses.get(
                url=f"{self.config.datagouv.url}/api/2/{object_class.namespace()}/search/",
                match=[
                    header_matcher(
                        {"X-Fields": f"data{{id,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"}
                    ),
                    query_param_matcher({"organization": org.id}, strict_match=False),
                ],
                json={
                    "data": [{"id": obj.id} for obj in objects if obj.organization == org],
                    "next_page": None,
                },
            )

    def mock_topic_elements(self, universe: Topic, object_class: type[TopicObject]) -> None:
        elements = universe.elements_of(object_class)
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/",
            match=[
                header_matcher(
                    {
                        "X-Fields": f"data{{id,element{{id}},{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                    }
                ),
                query_param_matcher({"class": object_class.object_name()}, strict_match=False),
            ],
            json={
                "data": [
                    {
                        "id": elem.id,
                        "element": {"class": object_class.object_name(), "id": elem.object.id},
                    }
                    for elem in elements
                ],
                "next_page": None,
            },
        )

    def mock_put_topic_elements(
        self, additions: Iterable[str], object_class: type[TopicObject]
    ) -> None:
        # TODO: support batching
        _ = self.responses.post(
            url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/",
            match=[
                header_matcher(
                    {"Content-Type": "application/json", "X-API-KEY": self.config.datagouv.token}
                ),
                json_params_matcher(
                    [
                        {"element": {"class": object_class.object_name(), "id": oid}}
                        for oid in additions
                    ]
                ),
            ],
        )

    def mock_delete_topic_elements(self, removals: Iterable[str]) -> None:
        for eid in removals:
            _ = self.responses.delete(
                url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/{eid}/",
                match=[header_matcher({"X-API-KEY": self.config.datagouv.token})],
            )

    def mock_get_bouquets(self, bouquets: Iterable[Topic]) -> None:
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/topics/",
            match=[
                header_matcher(
                    {
                        "X-API-KEY": self.config.datagouv.token,
                        "X-Fields": f"data{{id,name,organization{{id,name,slug}},slug,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page",
                    }
                ),
                query_param_matcher({"tag": self.config.tag, "include_private": "yes"}),
            ],
            json={"data": [asdict(b) for b in bouquets], "next_page": None},
        )
