import sys
import yaml

from pathlib import Path

import requests

from minicli import cli, run

from ecospheres_universe.feed_universe import ApiHelper, get_grist_orgs


@cli
def check_sync(*universe: Path, fail_on_errors: bool = False):
    print("Running check of universe sync...")

    conf = {}
    for u in universe:
        conf.update(yaml.safe_load(u.read_text()))

    url = conf['api']['url']
    api = ApiHelper(url, "no-token-needed", fail_on_errors=fail_on_errors, dry_run=True)

    grist_orgs = get_grist_orgs(conf['grist_url'], conf['env'])
    grist_orgs = sorted(grist_orgs, key=lambda o: o.slug)

    topic_slug = conf['topic']
    topic_id = api.get_topic_id(topic_slug)

    orgs = set()
    for org in grist_orgs:
        try:
            api_org = api.get_organization(org.slug)
            orgs.add((api_org["id"], api_org["name"]))
        except requests.exceptions.HTTPError:
            print(f"Unknown organization '{org.slug}'", file=sys.stderr)

    has_errors = 0
    for org_id, org_name in orgs:
        datasets_wo_es = api.get_topic_datasets_count(topic_id, org_id, use_es=False)
        datasets_w_es = api.get_topic_datasets_count(topic_id, org_id, use_es=True)
        if datasets_w_es == datasets_wo_es:
            print(f"✅ ({datasets_w_es}) — {org_name}")
        else:
            has_errors += 1
            print(f"❌ ({datasets_w_es} / {datasets_wo_es}) — {org_name}", file=sys.stderr)

    if has_errors:
        print(f"\n{has_errors} organizations are NOT in sync.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run(check_sync)
