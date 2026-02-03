from collections.abc import Iterable, Sequence
from copy import copy
from dataclasses import asdict

from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import (
    INACTIVE_OBJECT_MARKERS,
    Dataservice,
    Dataset,
    Organization,
    Owned,
    Topic,
    TopicElement,
    TopicObject,
)
from ecospheres_universe.util import uniquify

from .util import cycle_n


class DatagouvMock:
    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config
        self._mockIdCounter = 0

    def _next_id(self) -> int:
        self._mockIdCounter += 1
        return self._mockIdCounter

    @staticmethod
    def get_organizations[T: Owned](objects: Iterable[T]) -> Sequence[Organization]:
        return list(uniquify(org for o in objects if (org := o.organization)))

    def mock_dataset(self, organization: Organization | None = None) -> Dataset:
        id = self._next_id()
        return Dataset(
            id=f"dataset-{id}",
            slug=f"dataset-{id}",
            title=f"Dataset {id}",
            organization=organization,
        )

    def mock_datasets(
        self, n: int, organizations: Iterable[Organization] | None = None
    ) -> Sequence[Dataset]:
        orgs = cycle_n(organizations or [None], n)
        return [self.mock_dataset(org) for org in orgs]

    def mock_dataservice(self, organization: Organization | None = None) -> Dataservice:
        id = self._next_id()
        return Dataservice(
            id=f"dataservice-{id}",
            slug=f"dataservice-{id}",
            title=f"Dataservice {id}",
            organization=organization,
        )

    def mock_dataservices(
        self, n: int, organizations: Iterable[Organization] | None = None
    ) -> Sequence[Dataservice]:
        orgs = cycle_n(organizations or [None], n)
        return [self.mock_dataservice(org) for org in orgs]

    def mock_organization(self) -> Organization:
        id = self._next_id()
        return Organization(
            id=f"organization-{id}",
            slug=f"organization-{id}",
            name=f"Organization {id}",
        )

    def mock_organizations(self, n: int) -> Sequence[Organization]:
        return [self.mock_organization() for _ in range(n)]

    def mock_topic(
        self, objects: Iterable[TopicObject] | None = None, organization: Organization | None = None
    ) -> Topic:
        id = self._next_id()
        topic = Topic(
            id=f"topic-{id}",
            slug=f"topic-{id}",
            name=f"Topic {id}",
        )
        if objects:
            topic._elements = [self.mock_element(o) for o in objects]
        if organization:
            topic.organization = organization
        return topic

    def mock_element(self, object: TopicObject) -> TopicElement:
        return TopicElement(f"topicelement-{self._next_id()}", object)

    def clone_topic(
        self,
        topic: Topic,
        add: Iterable[TopicObject] | None = None,
        remove: Iterable[TopicObject] | None = None,
    ) -> Topic:
        t = copy(topic)
        if add:
            t._elements += [self.mock_element(obj) for obj in add]
        if remove:
            r = {r.id for r in remove}
            t._elements = [e for e in t._elements if e.object.id not in r]
        return t

    def mock_get_organization(self, universe: Topic) -> None:
        orgs = self.get_organizations(universe.objects())
        for org in orgs:
            _ = self.responses.get(
                url=f"{self.config.datagouv.url}/api/1/organizations/{org.slug}/",
                json={"id": org.id, "slug": org.slug, "name": org.name},
            )

    def mock_get_organization_object_ids(
        self, universe: Topic, object_class: type[TopicObject]
    ) -> None:
        orgs = self.get_organizations(universe.objects())
        elements = universe.elements(object_class)
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
                    "data": [{"id": e.object.id} for e in elements if e.object.organization == org],
                    "next_page": None,
                },
            )

    def mock_topic_elements(self, universe: Topic, object_class: type[TopicObject]) -> None:
        elements = universe.elements(object_class)
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
                        "id": e.id,
                        "element": {"class": object_class.object_name(), "id": e.object.id},
                    }
                    for e in elements
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
