from collections.abc import Iterable
from dataclasses import asdict, dataclass, field

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
from ecospheres_universe.grist import GristEntry
from ecospheres_universe.util import uniquify


@dataclass
class Proxy:
    object: DatagouvObject
    children: list[TopicObject] = field(default_factory=list)


class DatagouvMock:
    responses: RequestsMock
    config: Config
    _id_counter: int
    _objects: dict[str, Dataset | Dataservice | Proxy]

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config
        self._id_counter = 0
        self._objects = {}

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    @staticmethod
    def organizations(objects: Iterable[Owned]) -> Iterable[Organization]:
        return uniquify(org for obj in objects if (org := obj.organization))

    def objects(self, ids: Iterable[str]) -> Iterable[TopicObject]:
        for id in ids:
            object = self._objects[id]
            if isinstance(object, Proxy):
                yield from object.children
            else:
                yield object

    def dataservice(
        self,
        organization: Organization | None = None,
    ) -> Dataservice:
        id = self._next_id()
        dataservice = Dataservice(
            id=f"dataservice-{id}",
            slug=f"dataservice-{id}",
            title=f"Dataservice {id}",
            organization=organization,
        )
        self._objects[dataservice.id] = dataservice
        if organization:
            proxy = self._objects[organization.id]
            assert isinstance(proxy, Proxy)
            proxy.children.append(dataservice)
        return dataservice

    def dataset(
        self,
        organization: Organization | None = None,
    ) -> Dataset:
        id = self._next_id()
        dataset = Dataset(
            id=f"dataset-{id}",
            slug=f"dataset-{id}",
            title=f"Dataset {id}",
            organization=organization,
        )
        self._objects[dataset.id] = dataset
        if organization:
            proxy = self._objects[organization.id]
            assert isinstance(proxy, Proxy)
            proxy.children.append(dataset)
        return dataset

    def organization(self) -> Organization:
        id = self._next_id()
        org = Organization(
            id=f"organization-{id}", slug=f"organization-{id}", name=f"Organization {id}"
        )
        self._objects[org.id] = Proxy(org, [])
        return org

    def topic(self, organization: Organization | None = None) -> Topic:
        id = self._next_id()
        topic = Topic(
            id=f"topic-{id}", slug=f"topic-{id}", name=f"Topic {id}", organization=organization
        )
        self._objects[topic.id] = Proxy(topic, [])
        return topic

    def universe(self, objects: Iterable[TopicObject] | None = None) -> Topic:
        topic = self.topic()
        if objects:
            topic.elements = [TopicElement(f"element-{obj.id}", obj) for obj in objects]
        return topic

    def universe_from(self, grist_universe: Iterable[GristEntry]) -> Topic:
        ids = [entry.identifier for entry in grist_universe]
        objects = self.objects(ids)
        return self.universe(objects)

    def mock(
        self,
        existing_universe: Topic,
        upcoming_universe: Topic,
        bouquets: Iterable[Topic] | None = None,
    ) -> None:
        # datagouv.get_organization()
        self.mock_get_organization(upcoming_universe)

        # datagouv.delete_all_topic_elements()
        # TODO: support reset=True

        for object_class in Topic.object_classes():
            upcoming_elements = upcoming_universe.elements_of(object_class)
            existing_elements = existing_universe.elements_of(object_class)

            # datagouv.get_organization_objects_ids()
            self.mock_get_organization_object_ids(upcoming_universe, object_class)

            # datagouv.get_topic_elements()
            self.mock_get_topic_elements(existing_universe, object_class)

            existing_object_ids = {e.object.id for e in existing_elements}
            upcoming_object_ids = {e.object.id for e in upcoming_elements}

            # datagouv.put_topic_elements()
            additions = sorted(
                {e.object.id for e in upcoming_elements if e.object.id not in existing_object_ids}
            )
            if additions:
                self.mock_put_topic_elements(additions, object_class)

            # datagouv.delete_topic_elements()
            removals = sorted(
                {e.id for e in existing_elements if e.object.id not in upcoming_object_ids}
            )
            self.mock_delete_topic_elements(removals)

        # datagouv.get_bouquets()
        self.mock_get_bouquets(bouquets or [])

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
        # Warning: orgs list must be computed over *all* orgs, not just those matching object_class,
        # because requests have to be mocked even if they don't return anything
        orgs = uniquify(org for obj in universe.objects if (org := obj.organization))
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

    def mock_get_topic_elements(self, universe: Topic, object_class: type[TopicObject]) -> None:
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
