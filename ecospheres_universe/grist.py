import json
import requests

from enum import auto, StrEnum
from typing import NamedTuple


class GristType(StrEnum):
    ORGANIZATION = auto()


class GristEntry(NamedTuple):
    type: GristType
    id: str
    category: str  # LATER: drop (backcompat ecologie for now)


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
                    type=GristType.ORGANIZATION,  # LATER: switch to fields.type
                    id=rec["fields"]["slug"],  # LATER: switch to fields.identifier
                    category=rec["fields"]["type"],  # LATER: fields.category
                )
                for rec in r.json()["records"]
            }
        )
