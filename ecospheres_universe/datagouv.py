import requests

from dataclasses import dataclass
from enum import auto, Enum, StrEnum
from functools import total_ordering
from typing import Any, Generator

from ecospheres_universe.util import (
    JSONObject,
    batched,
    elapsed,
    elapsed_and_count,
    normalize_string,
    uniquify,
    verbose_print,
)


session = requests.Session()


class ObjectType(StrEnum):
    ORGANIZATION = auto()


class ElementClass(Enum):
    Dataset = "datasets"
    Dataservice = "dataservices"


@dataclass(frozen=True)
class Element:
    id: str
    object_id: str


@total_ordering
@dataclass(frozen=True)
class Organization:
    id: str
    name: str
    slug: str

    def __lt__(self, other):
        self_name = normalize_string(self.name)
        other_name = normalize_string(other.name)
        return self_name < other_name or (self_name == other_name and self.slug < other.slug)


@dataclass(frozen=True)
class Topic:
    id: str
    name: str
    organization: Organization | None


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
    def get_organization_object_ids(self, org_id: str, element_class: ElementClass) -> list[str]:
        url = f"{self.base_url}/api/2/{element_class.value}/search/?organization={org_id}&page_size=1000"
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
        self, topic_id_or_slug: str, element_class: ElementClass
    ) -> list[Element]:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/?class={element_class.name}&page_size=1000"
        objs = self._get_objects(url=url, fields=["id", "element{id}"])
        return [Element(id=o["id"], object_id=o["element"]["id"]) for o in objs]

    @elapsed_and_count
    def put_topic_elements(
        self,
        topic_id_or_slug: str,
        element_class: ElementClass,
        object_ids: list[str],
        batch_size: int = 0,
    ) -> None:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
        batches = batched(object_ids, batch_size) if batch_size else [object_ids]
        for batch in batches:
            data = [{"element": {"class": element_class.name, "id": id}} for id in batch]
            if not self.dry_run:
                session.post(url, json=data, headers=headers).raise_for_status()

    @elapsed
    def delete_topic_elements(self, topic_id_or_slug: str, element_ids: list[str]) -> None:
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
    def get_bouquets(self, universe_tag: str, include_private: bool = True) -> list[Topic]:
        """Fetch all bouquets (topics) tagged with the universe tag"""
        url = f"{self.base_url}/api/2/topics/?tag={universe_tag}"
        headers = {}
        if include_private:
            url = f"{url}&include_private=yes"
            headers["X-API-KEY"] = self.token
        objs = self._get_objects(
            url=url, headers=headers, fields=["id", "name", "organization{id,name,slug}"]
        )
        return [
            Topic(
                id=d["id"],
                name=d["name"],
                organization=Organization(id=o["id"], name=o["name"], slug=o["slug"])
                if (o := d.get("organization"))
                else None,
            )
            for d in objs
        ]

    def _get_objects(
        self, url: str, headers: dict[str, Any] | None = None, fields: list[str] | None = None
    ) -> Generator[JSONObject]:
        try:
            headers = dict(headers or {})  # local copy
            if fields:
                f = uniquify(["id", *fields])
                headers["X-Fields"] = "data{" + ",".join(f) + "},next_page"
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

    def _is_active(object: JSONObject) -> bool:
        return not any(object.get(m) for m in INACTIVE_OBJECT_MARKERS)
