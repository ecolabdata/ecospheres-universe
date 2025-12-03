import requests

from enum import Enum
from typing import Any, Callable, NamedTuple

from ecospheres_universe.util import elapsed, elapsed_and_count, verbose_print


session = requests.Session()


class ElementClass(Enum):
    Dataset = "datasets"
    Dataservice = "dataservices"


class Element(NamedTuple):
    id: str
    object_id: str


class Organization(NamedTuple):
    id: str
    name: str
    slug: str
    type: str | None


class DatagouvApi:
    def __init__(
        self, base_url: str, token: str, fail_on_errors: bool = False, dry_run: bool = False
    ):
        self.base_url = base_url
        self.token = token
        self.fail_on_errors = fail_on_errors
        self.dry_run = dry_run
        print(f"API for {self.base_url} ready.")

    def get_organization(self, org_id_or_slug: str) -> dict[str, Any]:
        url = f"{self.base_url}/api/1/organizations/{org_id_or_slug}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()

    def get_organization_objects_ids(self, org_id: str, element_class: ElementClass) -> list[str]:
        url = f"{self.base_url}/api/2/{element_class.value}/search/?organization={org_id}&page_size=1000"
        xfields = "data{id,archived,archived_at,deleted,deleted_at,private,extras{geop:dataset_id}}"

        def filter_objects(data: list[dict[str, Any]]) -> set[str]:
            return {
                d["id"]
                for d in data
                if not bool(
                    # dataset.archived
                    d.get("archived")
                    # dataservice.archived_at
                    or d.get("archived_at")
                    # dataset.deleted
                    or d.get("deleted")
                    # dataservice.deleted_at
                    or d.get("deleted_at")
                    # (dataset|dataservice).private
                    or d.get("private")
                    # dataset.extras[geop:dataset_id]
                    or d.get("extras")
                )
            }

        return self._get_objects_ids(url, filter_objects, xfields=xfields)

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

    # TODO: use _get_objects
    def get_topic_elements(
        self, topic_id_or_slug: str, element_class: ElementClass
    ) -> list[Element]:
        elements = list[Element]()
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/?class={element_class.name}&page_size=1000"
        while True:
            r = session.get(url)
            r.raise_for_status()
            data = r.json()
            elements.extend(
                [Element(id=elt["id"], object_id=elt["element"]["id"]) for elt in data["data"]]
            )
            url = data.get("next_page")
            if not url:
                break
        return elements

    @elapsed_and_count
    def put_topic_elements(
        self, topic_id_or_slug: str, element_class: ElementClass, objects_ids: list[str]
    ) -> list[str]:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
        data = [{"element": {"class": element_class.name, "id": id}} for id in objects_ids]
        if not self.dry_run:
            session.post(url, json=data, headers=headers).raise_for_status()
        return objects_ids

    @elapsed
    def delete_topic_elements(self, topic_id_or_slug: str, elements_ids: list[str]) -> None:
        for element_id in elements_ids:
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

    # TODO: use _get_objects
    @elapsed_and_count
    def get_bouquets(self, universe_tag: str, include_private: bool = True) -> list[dict[str, Any]]:
        """Fetch all bouquets (topics) tagged with the universe tag"""
        bouquets = list[dict[str, Any]]()
        headers = {}
        url = f"{self.base_url}/api/2/topics/?tag={universe_tag}"
        if include_private:
            url = f"{url}&include_private=yes"
            headers["X-API-KEY"] = self.token
        while url:
            r = session.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            bouquets.extend(data["data"])
            url = data.get("next_page")
        return bouquets

    @elapsed_and_count
    def _get_objects_ids(
        self, url: str, func: Callable[[list[dict[str, Any]]], set[str]], xfields: str = "data{id}"
    ) -> list[str]:
        objects_ids = set[str]()
        try:
            headers = {"X-Fields": f"{xfields},next_page"}
            while True:
                r = session.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
                objects_ids |= func(data["data"])
                url = data.get("next_page")
                if not url:
                    break
        except requests.HTTPError as e:
            if self.fail_on_errors:
                raise
            verbose_print(e)
        return list(objects_ids)
