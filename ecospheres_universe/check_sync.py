import os
import sys

from pathlib import Path

from minicli import cli, run

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvApi, ObjectType, Organization
from ecospheres_universe.grist import GristApi


@cli
def check_sync(universe: Path, *extra_configs: Path):
    """Check universe sync.

    :universe: Universe yaml config file
    :extra_configs: Additional config files (optional)
    """
    print("Running check of universe sync...")

    conf = Config.from_files(universe, *extra_configs)

    datagouv = DatagouvApi(
        base_url=conf.datagouv.url,
        token="no-token-needed",
        fail_on_errors=False,
        dry_run=True,
    )

    grist = GristApi(
        base_url=conf.grist.url,
        token=os.getenv("GRIST_API_KEY", conf.grist.token),
    )

    topic_id = datagouv.get_topic_id(conf.topic)

    orgs = set[Organization]()
    grist_orgs = [e for e in grist.get_entries() if e.type is ObjectType.ORGANIZATION]
    for grist_org in grist_orgs:
        org = datagouv.get_organization(grist_org.identifier)
        if not org:
            print(f"Unknown organization {grist_org.identifier}", file=sys.stderr)
            continue
        orgs.add(org)

    nb_errors = 0
    for org in sorted(orgs):
        datasets_wo_es = datagouv.get_topic_datasets_count(topic_id, org.id, use_search=False)
        datasets_w_es = datagouv.get_topic_datasets_count(topic_id, org.id, use_search=True)
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
