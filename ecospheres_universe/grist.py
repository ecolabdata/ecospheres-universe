import json
import requests

from typing import NamedTuple

from ecospheres_universe.datagouv import ObjectType


class GristEntry(NamedTuple):
    type: ObjectType
    id: str
    kind: str | None  # LATER: drop (backcompat ecologie for now)


class GristApi:
    def __init__(self, base_url: str, env: str):
        self.base_url = base_url
        self.env = env

    def get_entries(self) -> list[GristEntry]:
        r = requests.get(
            self.base_url, params={"filter": json.dumps({"env": [self.env]}), "limit": 0}
        )
        r.raise_for_status()
        return list(  # deduplicated list
            {
                GristEntry(
                    type=ObjectType.ORGANIZATION,  # LATER: ObjectType(rec["fields"]["type"])
                    id=rec["fields"]["slug"],  # LATER: rec["fields"]["identifier"]
                    kind=rec["fields"].get("type"),  # LATER: rec["fields"].get("kind"), then drop
                )
                for rec in r.json()["records"]
            }
        )
