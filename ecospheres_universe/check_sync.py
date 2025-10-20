import sys
import yaml

from pathlib import Path
from typing import NamedTuple

import requests

from minicli import cli, run

from ecospheres_universe.feed_universe import ApiHelper, get_grist_orgs


class Organization(NamedTuple):
    id: str
    name: str


@cli
def check_sync(universe: Path, *extra_configs: Path):
    """Check universe sync.

    :universe: Universe yaml config file
    :extra_configs: Additional config files (optional)
    """
    print("Running check of universe sync...")

    conf = {}
    for u in (universe,) + extra_configs:
        conf.update(yaml.safe_load(u.read_text()))

    url = conf['api']['url']
    api = ApiHelper(url, "no-token-needed", fail_on_errors=False, dry_run=True)

    grist_orgs = get_grist_orgs(conf['grist_url'], conf['env'])
    grist_orgs = sorted(grist_orgs, key=lambda o: o.slug)

    topic_slug = conf['topic']
    topic_id = api.get_topic_id(topic_slug)

    orgs: set[Organization] = set()
    for org in grist_orgs:
        try:
            api_org = api.get_organization(org.slug)
            orgs.add(Organization(id=api_org["id"], name=api_org["name"]))
        except requests.exceptions.HTTPError:
            print(f"Unknown organization '{org.slug}'", file=sys.stderr)

    nb_errors = 0
    for org in orgs:
        datasets_wo_es = api.get_topic_datasets_count(topic_id, org.id, use_search=False)
        datasets_w_es = api.get_topic_datasets_count(topic_id, org.id, use_search=True)
        if datasets_w_es == datasets_wo_es:
            print(f"✅ ({datasets_w_es}) — {org.name}")
        else:
            nb_errors += 1
            print(f"❌ ({datasets_w_es} / {datasets_wo_es}) — {org.name}", file=sys.stderr)

    if nb_errors:
        print(f"\n{nb_errors} organizations are NOT in sync.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run(check_sync)
