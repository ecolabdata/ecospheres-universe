import json
import requests

from typing import NamedTuple


class Organization(NamedTuple):
    slug: str
    type: str


def get_grist_orgs(grist_url: str, env: str) -> list[Organization]:
    r = requests.get(grist_url, params={"filter": json.dumps({"env": [env]}), "limit": 0})
    r.raise_for_status()
    # deduplicated list
    return list(
        {
            o["fields"]["slug"]: Organization(slug=o["fields"]["slug"], type=o["fields"]["type"])
            for o in r.json()["records"]
        }.values()
    )
