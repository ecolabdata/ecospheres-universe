import inspect
import requests
import sys

from abc import ABC
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import total_ordering
from itertools import batched
from typing import get_args, Any, TypeAlias

from ecospheres_universe.util import (
    JSONObject,
    elapsed,
    elapsed_and_count,
    normalize_string,
    uniquify,
    verbose_print,
)


session = requests.Session()


@dataclass
class DatagouvObject(ABC):
    """Base class for datagouv objects."""

    id: str
    slug: str | None = None

    @staticmethod
    def class_from_name(name: str) -> type["DatagouvObject"]:
        for clazz_name, clazz in inspect.getmembers(
            sys.modules[__name__], predicate=inspect.isclass
        ):
            if clazz_name.lower() == name.lower() and issubclass(clazz, DatagouvObject):
                return clazz
        raise TypeError(f"{name} is not a DatagouvObject")

    @classmethod
    def object_name(cls) -> str:
        """
        Name of the object class as declared in `udata.udata.core.*.models`.
        Override if different from `cls.__name__`.
        """
        return cls.__name__

    @classmethod
    def namespace(cls) -> str:
        """
        API namespace for the object.
        Override if different from plural lowercased `object_name()`.
        """
        return f"{cls.object_name().lower()}s"

    def as_json(self) -> JSONObject:
        return {
            "id": self.id,
            "slug": self.slug,
        }


@total_ordering
@dataclass
class Organization(DatagouvObject):
    name: str | None = None

    def __hash__(self) -> int:
        return hash(self.id)

    def __lt__(self, other: "Organization") -> bool:
        # TODO: replace assert guards with refresh() when we move to datagouv-client
        assert self.name is not None
        assert other.name is not None
        assert self.slug is not None
        assert other.slug is not None
        self_name = normalize_string(self.name)
        other_name = normalize_string(other.name)
        return self_name < other_name or (self_name == other_name and self.slug < other.slug)

    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "name": self.name,
        }


@dataclass
class OwnedObject(DatagouvObject, ABC):
    """Base class for datagouv objects that can be owned by an organization."""

    organization: Organization | None = None

    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "organization": self.organization.as_json() if self.organization else None,
        }


@dataclass
class RecordObject(OwnedObject):
    title: str | None = None

    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "title": self.title,
        }


@dataclass
class Dataset(RecordObject):
    pass


@dataclass
class Dataservice(RecordObject):
    pass


TopicObject: TypeAlias = Dataset | Dataservice


@dataclass
class TopicElement:
    id: str
    object: TopicObject


@dataclass
class Topic(OwnedObject):
    name: str | None = None
    _elements: list[TopicElement] = field(default_factory=list)

    @classmethod
    def object_classes(cls) -> tuple[type[TopicObject]]:
        return get_args(TopicObject)

    def elements(self, object_class: type[TopicObject] | None = None) -> Sequence[TopicElement]:
        elems = self._elements
        if object_class:
            elems = filter(lambda elem: type(elem.object) is object_class, elems)
        return list(elems)

    def objects(self, object_class: type[TopicObject] | None = None) -> Sequence[TopicObject]:
        return [elem.object for elem in self.elements(object_class)]

    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "name": self.name,
        }


INACTIVE_OBJECT_MARKERS = [
    "archived",  # dataset
    "archived_at",  # dataservice
    "deleted",  # dataset
    "deleted_at",  # dataservice
    "private",  # dataset,dataservice
    "extras{geop:dataset_id}",  # dataset
]


class DatagouvApi:
    def __init__(
        self, base_url: str, token: str, fail_on_errors: bool = False, dry_run: bool = False
    ):
        self.base_url = base_url
        self.token = token
        self.fail_on_errors = fail_on_errors
        self.dry_run = dry_run
        print(f"API for {self.base_url} ready.")

    def get_organization(self, org_id_or_slug: str) -> Organization | None:
        url = f"{self.base_url}/api/1/organizations/{org_id_or_slug}/"
        r = session.get(url)
        if not r.ok:
            return None
        data = r.json()
        return Organization(id=data["id"], name=data["name"], slug=data["slug"])

    @elapsed_and_count
    def get_organization_object_ids(
        self, org_id: str, object_class: type[DatagouvObject]
    ) -> Sequence[str]:
        url = f"{self.base_url}/api/2/{object_class.namespace()}/search/?organization={org_id}&page_size=1000"
        objs = self._get_objects(url=url, fields=INACTIVE_OBJECT_MARKERS)
        return uniquify(o["id"] for o in objs if DatagouvApi._is_active(o))

    def get_topic_id(self, topic_id_or_slug: str) -> str:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()["id"]

    def get_topic_datasets_count(self, topic_id: str, org_id: str, use_search: bool = False) -> int:
        url = f"{self.base_url}/api/2/datasets{'/search' if use_search else ''}/?topic={topic_id}&organization={org_id}&page_size=1"
        r = session.get(url)
        r.raise_for_status()
        return int(r.json()["total"])

    @elapsed_and_count
    def get_topic_elements(
        self, topic_id_or_slug: str, object_class: type[TopicObject]
    ) -> Sequence[TopicElement]:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/?class={object_class.object_name()}&page_size=1000"
        objs = self._get_objects(url=url, fields=["id", "element{id}"])
        return [TopicElement(id=o["id"], object=object_class(o["element"]["id"])) for o in objs]

    @elapsed_and_count
    def put_topic_elements(
        self,
        topic_id_or_slug: str,
        object_class: type[TopicObject],
        object_ids: Iterable[str],
        batch_size: int = 0,
    ) -> None:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
        batches = batched(object_ids, batch_size) if batch_size else [object_ids]
        for batch in batches:
            data = [{"element": {"class": object_class.object_name(), "id": id}} for id in batch]
            if not self.dry_run:
                session.post(url, json=data, headers=headers).raise_for_status()

    @elapsed
    def delete_topic_elements(self, topic_id_or_slug: str, element_ids: Iterable[str]) -> None:
        for element_id in element_ids:
            try:
                url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/{element_id}/"
                headers = {"X-API-KEY": self.token}
                if not self.dry_run:
                    session.delete(url, headers=headers).raise_for_status()
            except requests.HTTPError as e:
                if self.fail_on_errors:
                    raise
                verbose_print(e)

    @elapsed
    def delete_all_topic_elements(self, topic_id_or_slug: str) -> None:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        if not self.dry_run:
            headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
            session.delete(url, headers=headers).raise_for_status()

    @elapsed_and_count
    def get_bouquets(self, universe_tag: str, include_private: bool = True) -> Sequence[Topic]:
        """Fetch all bouquets (topics) tagged with the universe tag"""
        url = f"{self.base_url}/api/2/topics/?tag={universe_tag}"
        headers = {}
        if include_private:
            url = f"{url}&include_private=yes"
            headers["X-API-KEY"] = self.token
        objs = self._get_objects(
            url=url, headers=headers, fields=["id", "name", "organization{id,name,slug}", "slug"]
        )
        return [
            Topic(
                id=d["id"],
                slug=d["slug"],
                name=d["name"],
                organization=Organization(id=o["id"], name=o["name"], slug=o["slug"])
                if (o := d.get("organization"))
                else None,
            )
            for d in objs
        ]

    def _get_objects(
        self, url: str, headers: dict[str, Any] | None = None, fields: Iterable[str] | None = None
    ) -> Iterable[JSONObject]:
        try:
            headers = dict(headers or {})  # local copy
            if fields:
                f = uniquify(["id", *fields])
                headers["X-Fields"] = f"data{{{','.join(f)}}},next_page"
            while True:
                r = session.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
                yield from data["data"]
                url = data.get("next_page")
                if not url:
                    return
        except requests.HTTPError as e:
            if self.fail_on_errors:
                raise
            verbose_print(e)

    @staticmethod
    def _is_active(object: JSONObject) -> bool:
        return not any(object.get(m) for m in INACTIVE_OBJECT_MARKERS)
