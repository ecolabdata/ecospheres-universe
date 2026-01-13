import json
import requests

from enum import auto, StrEnum
from typing import NamedTuple


class GristType(StrEnum):
    ORGANIZATION = auto()


class GristEntry(NamedTuple):
    type: GristType
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
                    type=GristType(rec["fields"]["type"]),
                    id=rec["fields"]["identifier"],
                    kind=rec["fields"].get("kind"),
                )
                for rec in r.json()["records"]
            }
        )
