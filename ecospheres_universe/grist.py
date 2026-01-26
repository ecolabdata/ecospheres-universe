import requests

from typing import NamedTuple

from ecospheres_universe.datagouv import ObjectType


class GristEntry(NamedTuple):
    type: ObjectType
    identifier: str
    category: str | None  # LATER: drop (backcompat ecologie for now)


class GristApi:
    def __init__(self, base_url: str, table: str, token: str):
        self.base_url = base_url
        self.table = table
        self.token = token

    def get_entries(self) -> list[GristEntry]:
        r = requests.get(
            f"{self.base_url}/tables/{self.table}/records",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            params={"limit": 0},
        )
        r.raise_for_status()
        return list(  # deduplicated list
            {
                GristEntry(
                    type=ObjectType(rec["fields"]["Type"]),
                    identifier=rec["fields"]["Identifiant"],
                    category=rec["fields"].get("Categorie"),
                )
                for rec in r.json()["records"]
            }
        )
