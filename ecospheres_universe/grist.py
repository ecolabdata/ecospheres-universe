import requests

from collections.abc import Sequence
from dataclasses import dataclass

from ecospheres_universe.datagouv import DatagouvObject
from ecospheres_universe.util import uniquify


@dataclass(frozen=True)
class GristEntry:
    object_class: type[DatagouvObject]
    identifier: str
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
        return uniquify(
            GristEntry(
                object_class=DatagouvObject.class_from_name(rec["fields"]["Type"]),
                identifier=rec["fields"]["Identifiant"],
                category=rec["fields"].get("Categorie"),
            )
            for rec in r.json()["records"]
        )
