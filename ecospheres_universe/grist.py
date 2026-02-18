import requests

from collections.abc import Sequence
from dataclasses import dataclass
from enum import auto, StrEnum

from ecospheres_universe.datagouv import DatagouvObject
from ecospheres_universe.util import uniquify


class GristAction(StrEnum):
    """
    Grist Action column values.
    The main use for this class is to map/validate the grist input before it is converted to a
    simple flag in GristEntry.exclude.
    """

    INCLURE = auto()
    EXCLURE = auto()


@dataclass(frozen=True, kw_only=True)
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
        return GristEntry(
            identifier=record["Identifiant"],
            object_class=DatagouvObject.class_from_name(record["Type"]),
            exclude=GristAction(record["Action"]) is GristAction.EXCLURE,
            category=record.get("Categorie"),
        )
