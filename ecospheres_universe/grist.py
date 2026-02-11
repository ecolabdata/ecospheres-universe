import requests

from collections.abc import Sequence
from dataclasses import dataclass
from enum import auto, StrEnum

from ecospheres_universe.datagouv import DatagouvObject
from ecospheres_universe.util import uniquify


class GristAction(StrEnum):
    INCLURE = auto()
    EXCLURE = auto()


@dataclass(frozen=True)
class GristEntry[T: DatagouvObject]:
    identifier: str
    object_class: type[T]
    exclude: bool = False
    category: str | None = None  # LATER: drop (backcompat ecologie for now)


class GristApi:
    def __init__(self, base_url: str, table: str, token: str):
        self.base_url = base_url
        self.table = table
        self.token = token

    def get_entries(self) -> Sequence[GristEntry]:
        r = requests.get(
            f"{self.base_url}/tables/{self.table}/records",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            params={"limit": 0},
        )
        r.raise_for_status()
        records = r.json()["records"]
        return uniquify(self._make_entry(rec["fields"]) for rec in records)

    @staticmethod
    def _make_entry(record: dict[str, str]) -> GristEntry:
        identifier = record["Identifiant"]
        object_class = DatagouvObject.class_from_name(record["Type"])
        exclude = GristAction(record["Action"]) is GristAction.EXCLURE
        category = record.get("Categorie")
        return GristEntry(identifier, object_class, exclude, category)
