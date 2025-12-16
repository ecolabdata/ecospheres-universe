import datetime
import json
import os
import time
import sys

from pathlib import Path
from shutil import copyfile
from typing import NamedTuple

from minicli import cli, run

from ecospheres_universe.datagouv import DatagouvApi, ElementClass
from ecospheres_universe.grist import GristApi, GristEntry, GristType
from ecospheres_universe.util import (
    load_configs,
    normalized_string,
    verbose_print,  # noqa: F401
)


REMOVALS_THRESHOLD = 1800


class Organization(NamedTuple):
    id: str
    name: str
    slug: str
    # LATER: rename type to category - WARNING! impacts dashboard-backend
    # + fetch from another grist (won't have all orgs in universe with other types being added)
    type: str = ""


def write_organizations_file(filename: str, orgs: list[Organization]):
    """Write organizations list to a JSON file in dist/"""
    print(f"Generating output file {filename} with {len(orgs)} entries...")
    with open(f"dist/{filename}", "w") as f:
        json.dump([o._asdict() for o in orgs], f, indent=2, ensure_ascii=False)


def get_universe_objects(
    datagouv: DatagouvApi,
    grist_entries: list[GristEntry],
    element_class: ElementClass,
    keep_empty: bool = False,
) -> tuple[list[str], list[Organization]]:
    objects_ids = list[str]()
    active_organizations = list[Organization]()
    for entry in grist_entries:
        # LATER: generalise to all entry.type => get_xxx_object_ids, with:
        #   - organization => multiple object_ids, single active_org
        #   - keyword, topic => multiple object_ids, multiple active_orgs
        #   - dataset, dataservice (when matching entry.type) => single object_id, single active_org
        # LATER: get_organization() not needed since we'll have entry.object_id
        assert entry.type is GristType.ORGANIZATION
        org = datagouv.get_organization(entry.slug)
        if not org:
            print(f"Unknown organization {entry.slug}", file=sys.stderr)
            continue
        verbose_print(f"Fetching {element_class.name} for organization {org.id}...")
        ids = datagouv.get_organization_object_ids(org.id, element_class)
        objects_ids += ids
        if keep_empty or ids:
            # LATER: get org info from get_xxx_object_ids (topics/tags can have multiple orgs)
            active_organizations.append(
                Organization(id=org.id, name=org.name, slug=org.slug, type=entry.category)
            )
    return objects_ids, active_organizations


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

    conf = load_configs(universe, *extra_configs)

    datagouv = DatagouvApi(
        base_url=conf["api"]["url"],
        token=os.getenv("DATAGOUV_API_KEY", conf["api"]["token"]),
        fail_on_errors=fail_on_errors,
        dry_run=dry_run,
    )

    grist = GristApi(conf["grist_url"], conf["env"])

    topic_slug = str(conf["topic"])
    bouquet_tag = str(conf["tag"])

    print(f"Starting at {datetime.datetime.now():%c}")
    if dry_run:
        print("*** DRY RUN ***")

    t_all = time.time()
    try:
        print(f"Processing topic {topic_slug}:")

        verbose_print("Fetching grist universe...")
        grist_entries = grist.get_grist_entries()

        if reset:
            print("Removing ALL elements from topic...")
            datagouv.delete_all_topic_elements(topic_slug)

        for element_class in ElementClass:
            # FIXME: move after "fetch existing"
            verbose_print(f"Fetching new topic {element_class}...")
            new_objects_ids, active_organizations = get_universe_objects(
                datagouv, grist_entries, element_class, keep_empty
            )
            print(f"Found {len(new_objects_ids)} new {element_class.value}.")

            # TODO: don't run if reset==True?
            verbose_print(f"Fetching existing topic {element_class}...")
            existing_elements = datagouv.get_topic_elements(topic_slug, element_class)
            existing_objects_ids = [e.object_id for e in existing_elements]
            print(f"Found {len(existing_objects_ids)} existing {element_class.name}.")

            additions = list(set(new_objects_ids) - set(existing_objects_ids))
            removals = list(set(existing_objects_ids) - set(new_objects_ids))
            if n := len(removals) > REMOVALS_THRESHOLD:
                raise Exception(f"Too many removals ({n} > {REMOVALS_THRESHOLD}), aborting.")

            print("Updating topic...")
            print(f"- Adding {len(additions)} {element_class.value}...")
            datagouv.put_topic_elements(topic_slug, element_class, additions, 1000)

            # TODO: don't run if reset==True?
            print(f"- Deleting {len(removals)} {element_class.value}...")
            elements_ids = [e.id for e in existing_elements if e.object_id in removals]
            datagouv.delete_topic_elements(topic_slug, elements_ids)

            write_organizations_file(
                f"organizations-{element_class.value}-{conf['env']}.json",
                sorted(active_organizations, key=lambda o: normalized_string(o.name)),
            )
    finally:
        print(f"Elapsed: {time.time() - t_all:.2f} s")

    # FIXME: remove when front uses the new file path
    # retrocompatibility
    copyfile(
        f"dist/organizations-datasets-{conf['env']}.json", f"dist/organizations-{conf['env']}.json"
    )

    # TODO: can this be handled by the main update loop? datasets/services in bouquets should also be in universe
    print("Fetching organizations from bouquets...")
    bouquets = datagouv.get_bouquets(bouquet_tag)
    bouquet_orgs = list(
        {
            Organization(id=o["id"], name=o["name"], slug=o["slug"])
            for b in bouquets
            if (o := b.get("organization"))
        }
    )
    write_organizations_file(
        f"organizations-bouquets-{conf['env']}.json",
        sorted(bouquet_orgs, key=lambda o: normalized_string(o.name)),
    )

    print(f"Done at {datetime.datetime.now():%c}")


if __name__ == "__main__":
    run(feed_universe)
