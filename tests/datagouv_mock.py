from collections.abc import Iterable, Sequence
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
from ecospheres_universe.grist import GristEntry
from ecospheres_universe.util import uniquify


@dataclass
class Proxy[T: DatagouvObject]:
    object: T
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

    @staticmethod
    def owning_organizations(*objects: Owned) -> Sequence[Organization]:
        return uniquify(org for obj in objects if (org := obj.organization))

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
        # Topic doesn't need a proxy since it stores its own elements, but using one for symmetry,
        # to avoid dealing with another variant in _objects
        self._objects[topic.id] = Proxy(topic, [])
        return topic

    def universe(self, objects: Iterable[TopicObject] | None = None) -> Topic:
        # Ignoring universe.organization since it's not used in tests so far
        topic = self.topic()
        if objects:
            topic.elements.extend(TopicElement(f"element-{obj.id}", obj) for obj in objects)
        return topic

    def universe_from(self, grist_universe: Iterable[GristEntry]) -> Topic:
        objects = self._leaf_objects(*(entry.identifier for entry in grist_universe))
        return self.universe(objects)

    def mock(
        self,
        existing_universe: list[TopicObject],
        grist_universe: list[GristEntry],
        bouquets: Iterable[Topic] | None = None,
    ) -> None:
        existing = self.universe(existing_universe)
        upcoming = self.universe_from(grist_universe)

        # datagouv.delete_all_topic_elements()
        # TODO: support reset=True

        for object_class in Topic.object_classes():
            upcoming_elements = upcoming.elements_of(object_class)
            existing_elements = existing.elements_of(object_class)

            for entry in grist_universe:
                # datagouv.get_upcoming_universe_perimeter()
                if entry.object_class is Organization:
                    self.mock_get_upcoming_universe_perimeter_organization(
                        entry.identifier, object_class
                    )

            # datagouv.get_topic_elements()
            self.mock_get_topic_elements(existing_elements, object_class)

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

    def mock_get_upcoming_universe_perimeter_organization(
        self, id: str, object_class: type[TopicObject]
    ) -> None:
        url = f"{self.config.datagouv.url}/api/1/organizations/{id}/"
        org = self._get_object(id, Organization)
        if not org:
            _ = self.responses.get(url=url, status=404)
            return

        _ = self.responses.get(url=url, json={"id": org.id, "slug": org.slug, "name": org.name})

        objects = self._leaf_objects_of(org.id, object_class=object_class)
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/{object_class.namespace()}/search/",
            match=[
                header_matcher(
                    {"X-Fields": f"data{{id,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"}
                ),
                query_param_matcher({"organization": org.id}, strict_match=False),
            ],
            json={
                "data": [{"id": obj.id} for obj in objects],
                "next_page": None,
            },
        )

    def mock_get_topic_elements(
        self, elements: Iterable[TopicElement], object_class: type[TopicObject]
    ) -> None:
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/",
            match=[
                header_matcher(
                    {
                        "X-Fields": f"data{{id,element{{id}},{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                    }
                ),
                query_param_matcher({"class": object_class.model_name()}, strict_match=False),
            ],
            json={
                "data": [
                    {
                        "id": elem.id,
                        "element": {"class": object_class.model_name(), "id": elem.object.id},
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
                        {"element": {"class": object_class.model_name(), "id": oid}}
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

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    def _get_object[T: DatagouvObject](self, id: str, _: type[T]) -> T | None:
        if object := self._objects.get(id):
            if isinstance(object, Proxy):
                return cast(T, object.object)
            else:
                return cast(T, object)

    def _leaf_objects(self, *ids: str) -> Sequence[TopicObject]:
        return list(self._leaf_objects_inner(*ids))

    def _leaf_objects_of[T: TopicObject](self, *ids: str, object_class: type[T]) -> Sequence[T]:
        # cast shouldn't be needed, but ty complains
        return [cast(T, obj) for obj in self._leaf_objects_inner(*ids) if type(obj) is object_class]

    def _leaf_objects_inner(self, *ids: str) -> Iterable[TopicObject]:
        for id in ids:
            if object := self._objects.get(id):
                if isinstance(object, Proxy):
                    yield from object.children
                else:
                    yield object
