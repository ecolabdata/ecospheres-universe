import datetime
import json
import os
import sys
import time

from dataclasses import asdict, dataclass
from pathlib import Path
from shutil import copyfile

from minicli import cli, run

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvApi, ElementClass, ObjectType, Organization
from ecospheres_universe.grist import GristApi, GristEntry
from ecospheres_universe.util import (
    verbose_print,  # noqa: F401
)


ADDITIONS_BATCH_SIZE = 1000
REMOVALS_THRESHOLD = 1800


@dataclass(frozen=True)
class UniverseOrg(Organization):
    type: str | None = None  # TODO: rename to category !! impacts dashboard-backend


def write_organizations_file(filepath: Path, orgs: list[UniverseOrg]):
    """Write organizations list to a JSON file in dist/"""
    print(f"Generating output file {filepath} with {len(orgs)} entries...")
    with filepath.open("w") as f:
        json.dump([asdict(o) for o in orgs], f, indent=2, ensure_ascii=False)


def get_upcoming_universe_perimeter(
    datagouv: DatagouvApi,
    grist_entries: list[GristEntry],
    element_class: ElementClass,
    keep_empty: bool = False,
) -> tuple[list[str], list[UniverseOrg]]:
    object_ids = set[str]()
    orgs = set[UniverseOrg]()

    def _update_perimeter(ids: list[str], org: Organization | None):
        object_ids.update(ids)
        if org and (keep_empty or ids):
            orgs.add(UniverseOrg(id=org.id, name=org.name, slug=org.slug, type=entry.kind))

    for entry in grist_entries:
        match entry.type:
            case ObjectType.ORGANIZATION:
                org = datagouv.get_organization(entry.id)
                if not org:
                    print(f"Unknown organization {entry.id}", file=sys.stderr)
                    continue
                verbose_print(f"Fetching {element_class.value} for organization {org.id}...")
                ids = datagouv.get_organization_object_ids(org.id, element_class)
                _update_perimeter(ids, org)
            case _:
                continue

    return list(object_ids), list(orgs)


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

    print(f"Starting at {datetime.datetime.now():%c}.")
    if dry_run:
        print("*** DRY RUN ***")

    t_all = time.time()
    try:
        print(f"Processing universe topic '{conf.topic}'.")

        verbose_print("Fetching grist universe definition...")
        grist_entries = grist.get_entries()
        print(f"Found {len(grist_entries)} entries in grist.")

        if reset:
            print("Removing ALL elements from topic...")
            datagouv.delete_all_topic_elements(conf.topic)

        for element_class in ElementClass:
            verbose_print(f"Fetching upcoming {element_class.value}...")
            upcoming_object_ids, upcoming_orgs = get_upcoming_universe_perimeter(
                datagouv, grist_entries, element_class, keep_empty
            )
            print(
                f"Found {len(upcoming_object_ids)} {element_class.value} matching the upcoming universe."
            )

            verbose_print(f"Fetching existing {element_class.value}...")
            existing_elements = datagouv.get_topic_elements(conf.topic, element_class)
            existing_object_ids = list({e.object_id for e in existing_elements})
            print(
                f"Found {len(existing_object_ids)} {element_class.value} currently in the universe."
            )

            verbose_print("Computing topic updates...")
            additions = sorted(set(upcoming_object_ids) - set(existing_object_ids))
            removals = sorted(set(existing_object_ids) - set(upcoming_object_ids))
            if (n := len(removals)) > REMOVALS_THRESHOLD:
                raise Exception(f"Too many removals ({n} > {REMOVALS_THRESHOLD}), aborting.")

            print("Updating topic:")
            print(f"- Adding {len(additions)} {element_class.value}...")
            datagouv.put_topic_elements(conf.topic, element_class, additions, ADDITIONS_BATCH_SIZE)

            print(f"- Deleting {len(removals)} {element_class.value}...")
            element_ids = [e.id for e in existing_elements if e.object_id in removals]
            datagouv.delete_topic_elements(conf.topic, element_ids)

            write_organizations_file(
                conf.output_dir / f"organizations-{element_class.value}-{conf.env}.json",
                sorted(upcoming_orgs),
            )

        # FIXME: remove when front uses the new file path
        # retrocompatibility
        copyfile(
            f"dist/organizations-datasets-{conf.env}.json", f"dist/organizations-{conf.env}.json"
        )

        # TODO: custom ecologie => make that an option?
        # TODO: can this be handled by the main update loop? datasets/services in bouquets should also be in universe?
        verbose_print("Fetching additional organizations from bouquets...")
        bouquets = datagouv.get_bouquets(conf.tag)
        print(f"Found {len(bouquets)} bouquets with the universe tag.")
        bouquet_orgs = list(
            {
                UniverseOrg(id=o.id, name=o.name, slug=o.slug, type=None)
                for b in bouquets
                if (o := b.organization)
            }
        )
        write_organizations_file(
            conf.output_dir / f"organizations-bouquets-{conf.env}.json",
            sorted(bouquet_orgs),
        )

    finally:
        print(f"Done at {datetime.datetime.now():%c} in {time.time() - t_all:.0f} seconds.")


if __name__ == "__main__":
    run(feed_universe)
