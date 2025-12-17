import datetime
import json
import os
import sys
import time

from collections import defaultdict
from pathlib import Path
from shutil import copyfile

import requests
import unicodedata

from minicli import cli, run

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvApi, ElementClass, Organization
from ecospheres_universe.grist import GristApi
from ecospheres_universe.util import (
    batched,
    verbose_print,  # noqa: F401
)


REMOVALS_THRESHOLD = 1800


def sort_orgs_by_name(orgs: list[Organization]) -> list[Organization]:
    """Sort organizations by name, ignoring diacritics"""
    return sorted(
        orgs,
        key=lambda o: unicodedata.normalize("NFKD", o.name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower(),
    )


def write_organizations_file(filepath: Path, orgs: list[Organization]):
    """Write organizations list to a JSON file in dist/"""
    print(f"Generating output file {filepath}...")
    with filepath.open("w") as f:
        json.dump([o._asdict() for o in orgs], f, indent=2, ensure_ascii=False)


def feed(
    conf: Config,
    keep_empty: bool = False,
    fail_on_errors: bool = False,
    dry_run: bool = False,
    reset: bool = False,
) -> None:
    datagouv = DatagouvApi(
        base_url=conf.api.url,
        token=os.getenv("DATAGOUV_API_KEY", conf.api.token),
        fail_on_errors=fail_on_errors,
        dry_run=dry_run,
    )

    grist = GristApi(conf.grist_url, conf.env)

    grist_orgs = grist.get_organizations()

    print(f"Starting at {datetime.datetime.now():%c}")
    if dry_run:
        print("*** DRY RUN ***")

    t_count: dict[ElementClass, int] = defaultdict(int)
    t_all = time.time()
    try:
        verbose_print(f"Getting existing datasets for topic '{conf.topic}'")
        orgs: list[Organization] = []

        for org in grist_orgs:
            verbose_print(f"Checking organization '{org.slug}'")
            try:
                api_org = datagouv.get_organization(org.slug)
                orgs.append(
                    Organization(
                        id=api_org["id"],
                        name=api_org["name"],
                        slug=org.slug,
                        type=org.type,
                    )
                )
            except requests.HTTPError:
                print(f"Unknown organization '{org.slug}'", file=sys.stderr)

        orgs = sort_orgs_by_name(orgs)

        print(f"Processing {len(orgs)} organizations...")

        if reset:
            print(f"Removing ALL elements from topic '{conf.topic}'")
            datagouv.delete_all_topic_elements(conf.topic)

        active_orgs: dict[ElementClass, list[Organization]] = defaultdict(list)

        print(f"Processing topic '{conf.topic}'")
        for element_class in ElementClass:
            new_objects_ids = list[str]()
            for org in orgs:
                verbose_print(f"Fetching {element_class.name} for organization '{org.slug}'...")
                objects_ids = datagouv.get_organization_objects_ids(org.id, element_class)
                if not objects_ids and not keep_empty:
                    verbose_print(f"Skipping empty organization '{org.slug}'")
                    continue
                t_count[element_class] += len(objects_ids)
                active_orgs[element_class].append(org)
                new_objects_ids += objects_ids

            existing_elements = datagouv.get_topic_elements(conf.topic, element_class)
            existing_objects_ids = set(e.object_id for e in existing_elements)
            print(
                f"Found {len(existing_objects_ids)} existing {element_class.name} in universe topic."
            )
            additions = list(set(new_objects_ids) - existing_objects_ids)
            removals = list(existing_objects_ids - set(new_objects_ids))
            if len(removals) > REMOVALS_THRESHOLD:
                raise Exception(f"Too many removals ({len(removals)}), aborting")
            print(f"Feeding {len(additions)} {element_class.value}...")
            for batch in batched(additions, 1000):
                _ = datagouv.put_topic_elements(conf.topic, element_class, batch)
            print(f"Removing {len(removals)} {element_class.value}...")
            elements_removals = [e.id for e in existing_elements if e.object_id in removals]
            datagouv.delete_topic_elements(conf.topic, elements_removals)

    finally:
        print(f"Elapsed: {time.time() - t_all:.2f} s")
        for element_class in ElementClass:
            print(f"Total count {element_class.value}: {t_count[element_class]}")

    for element_class in ElementClass:
        write_organizations_file(
            conf.output_dir / f"organizations-{element_class.value}-{conf.env}.json",
            active_orgs[element_class],
        )

    # FIXME: remove when front uses the new file path
    # retrocompatibility
    copyfile(f"dist/organizations-datasets-{conf.env}.json", f"dist/organizations-{conf.env}.json")

    # Build a list of organizations from the list of bouquets
    print("Fetching organizations from bouquets...")
    bouquets = datagouv.get_bouquets(conf.tag)
    bouquet_orgs = list(
        {
            o["id"]: Organization(id=o["id"], name=o["name"], slug=o["slug"], type=None)
            for b in bouquets
            if (o := b.get("organization"))
        }.values()
    )
    bouquet_orgs = sort_orgs_by_name(bouquet_orgs)
    write_organizations_file(
        conf.output_dir / f"organizations-bouquets-{conf.env}.json", bouquet_orgs
    )

    print(f"Done at {datetime.datetime.now():%c}")


@cli
def feed_universe(
    universe: Path,
    *extra_configs: Path,
    keep_empty: bool = False,
    fail_on_errors: bool = False,
    dry_run: bool = False,
    reset: bool = False,
    verbose: bool = False,
):
    """Feed the universe with datasets and dataservices from organizations.

    :universe: Universe yaml config file
    :extra_configs: Additional config files (optional)
    :keep_empty: Keep empty organizations in the list
    :fail_on_errors: Fail the run on http errors
    :dry_run: Perform a trial run without actual feeding
    :reset: Empty topic before refeeding it
    :verbose: Enable verbose mode
    """
    global verbose_print
    if verbose:
        verbose_print = print

    conf = Config.from_files(universe, *extra_configs)

    feed(
        conf=conf,
        keep_empty=keep_empty,
        fail_on_errors=fail_on_errors,
        dry_run=dry_run,
        reset=reset,
    )


if __name__ == "__main__":
    run(feed_universe)
