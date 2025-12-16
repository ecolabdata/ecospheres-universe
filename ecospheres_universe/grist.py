import json
import requests

from enum import auto, StrEnum
from typing import NamedTuple


class GristType(StrEnum):
    ORGANIZATION = auto()


class GristEntry(NamedTuple):
    type: GristType
    slug: str  # LATER: switch to id
    category: str  # LATER: get rid of it


class GristApi:
    def __init__(self, base_url: str, env: str):
        self.base_url = base_url
        self.env = env

    def get_grist_entries(self) -> list[GristEntry]:
        r = requests.get(
            self.base_url, params={"filter": json.dumps({"env": [self.env]}), "limit": 0}
        )
        r.raise_for_status()
        # TODO: raise on duplicate GristEntry
        # deduplicated list
        return list(
            {
                rec["fields"]["slug"]: GristEntry(
                    type=GristType.ORGANIZATION,  # LATER: fields.type
                    slug=rec["fields"]["slug"],
                    category=rec["fields"]["type"],  # LATER: fields.category
                )
                for rec in r.json()["records"]
            }.values()
        )
