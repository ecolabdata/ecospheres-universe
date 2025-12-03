import json
import requests

from typing import NamedTuple


class Organization(NamedTuple):
    slug: str
    type: str


class GristApi:
    def __init__(self, base_url: str, env: str):
        self.base_url = base_url
        self.env = env

    def get_organizations(self) -> list[Organization]:
        r = requests.get(
            self.base_url, params={"filter": json.dumps({"env": [self.env]}), "limit": 0}
        )
        r.raise_for_status()
        # deduplicated list
        return list(
            {
                o["fields"]["slug"]: Organization(
                    slug=o["fields"]["slug"], type=o["fields"]["type"]
                )
                for o in r.json()["records"]
            }.values()
        )
