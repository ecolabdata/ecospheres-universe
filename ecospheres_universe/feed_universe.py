import datetime
import functools
import json
import os
import sys
import time

from collections import defaultdict
from enum import Enum
from pathlib import Path
from shutil import copyfile
from typing import NamedTuple

import requests
import unicodedata
import yaml
from minicli import cli, run

REMOVALS_THRESHOLD = 1800

session = requests.Session()

# noop unless args.verbose is set
def verbose_print(*args, **kwargs):
    return None


class ElementClass(Enum):
    Dataset = "datasets"
    Dataservice = "dataservices"


class Organization(NamedTuple):
    id: str
    name: str
    slug: str
    type: str


class GristOrganization(NamedTuple):
    slug: str
    type: str


class ElementJoin(NamedTuple):
    element_id: str
    object_id: str


def elapsed_and_count(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        val = None
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(f"<{func.__name__}: count={len(val or [])}, elapsed={time.time() - t:.2f}s>")
        return val
    return wrapper_decorator

def elapsed(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(f"<{func.__name__}: elapsed={time.time() - t:.2f}s>")
        return val
    return wrapper_decorator


def batched(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]


def sort_orgs_by_name(orgs: list[Organization]) -> list[Organization]:
    """Sort organizations by name, ignoring diacritics"""
    return sorted(orgs, key=lambda o: unicodedata.normalize("NFKD", o.name).encode("ascii", "ignore").decode("ascii").lower())


def write_organizations_file(filename: str, orgs: list[Organization]):
    """Write organizations list to a JSON file in dist/"""
    print(f"Generating output file {filename}...")
    with open(f"dist/{filename}", "w") as f:
        json.dump([o._asdict() for o in orgs], f, indent=2, ensure_ascii=False)


def get_grist_orgs(grist_url: str, env: str) -> list[GristOrganization]:
    r = requests.get(grist_url, params={'filter': json.dumps({'env': [env]}), 'limit': 0})
    r.raise_for_status()
    # deduplicated list
    return list({
        o['fields']['slug']: GristOrganization(slug=o['fields']['slug'], type=o['fields']['type'])
        for o in r.json()['records']
    }.values())

class IndentedDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentedDumper, self).increase_indent(flow, False)


class ApiHelper:

    def __init__(self, base_url, token, fail_on_errors=False, dry_run=False):
        self.base_url = base_url
        self.token = token
        self.fail_on_errors = fail_on_errors
        self.dry_run = dry_run
        print(f"API for {self.base_url} ready.")

    @elapsed_and_count
    def get_objects(self, url, func, xfields='data{id}'):
        objects = set()
        try:
            headers={'X-Fields': f"{xfields},next_page"}
            while True:
                r = session.get(url, headers=headers)
                r.raise_for_status()
                c = r.json()
                objects |= func(c)
                url = c.get('next_page')
                if not url:
                    break
        except requests.exceptions.HTTPError as e:
            if self.fail_on_errors:
                raise
            verbose_print(e)
        return list(objects)

    def get_organization(self, org: str):
        url = f"{self.base_url}/api/1/organizations/{org}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()

    def get_topic_id(self, topic_slug: str) -> str:
        url = f"{self.base_url}/api/2/topics/{topic_slug}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()['id']

    @elapsed_and_count
    def get_bouquets(self, universe_tag: str, include_private: bool = True) -> list[dict]:
        """Fetch all bouquets (topics) tagged with the universe tag"""
        bouquets = []
        headers = {}
        url = f"{self.base_url}/api/2/topics/?tag={universe_tag}"
        if include_private:
            url = f"{url}&include_private=yes"
            headers["X-API-KEY"] = self.token
        while url:
            r = session.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            bouquets.extend(data["data"])
            url = data.get("next_page")
        return bouquets

    def get_organization_objects(self, org_id: str, element_class: ElementClass) -> list[str]:
        url = f"{self.base_url}/api/2/{element_class.value}/search/?organization={org_id}&page_size=1000"
        xfields = 'data{id,archived,archived_at,deleted,deleted_at,private,extras{geop:dataset_id}}'
        def filter_objects(c):
            return {
                d["id"]
                for d in c["data"]
                if not bool(
                    # dataset.archived
                    d.get("archived")
                    # dataservice.archived_at
                    or d.get("archived_at")
                    # dataset.deleted
                    or d.get("deleted")
                    # dataservice.deleted_at
                    or d.get("deleted_at")
                    # (dataset|dataservice).private
                    or d.get("private")
                    # dataset.extras[geop:dataset_id]
                    or d.get("extras")
                )
            }
        return self.get_objects(url, filter_objects, xfields=xfields)

    def get_topic_datasets_count(self, topic_id: str, org_id: str, use_search: bool = False):
        url = f"{self.base_url}/api/2/datasets{'/search' if use_search else ''}/?topic={topic_id}&organization={org_id}&page_size=1"
        r = session.get(url)
        r.raise_for_status()
        return r.json()["total"]

    def get_topic_elements(self, topic: str, element_class: ElementClass) -> list[ElementJoin]:
        elements = []
        url = f"{self.base_url}/api/2/topics/{topic}/elements/?class={element_class.name}&page_size=1000"
        while True:
            r = session.get(url)
            r.raise_for_status()
            c = r.json()
            elements.extend([ElementJoin(element_id=elt['id'], object_id=elt['element']['id']) for elt in c['data']])
            url = c.get('next_page')
            if not url:
                break
        return elements

    @elapsed_and_count
    def put_topic_elements(self, topic, element_class: ElementClass, objects):
        url = f"{self.base_url}/api/2/topics/{topic}/elements/"
        headers = {'Content-Type': 'application/json', 'X-API-KEY': self.token}
        data = [{'element': {'class': element_class.name, 'id': d}} for d in objects]
        if not self.dry_run:
            session.post(url, json=data, headers=headers).raise_for_status()
        return objects

    @elapsed
    def delete_topic_elements(self, topic, elements):
        for elt in elements:
            try:
                url = f"{self.base_url}/api/2/topics/{topic}/elements/{elt}/"
                headers = {'X-API-KEY': self.token}
                if not self.dry_run:
                    session.delete(url, headers=headers).raise_for_status()
            except requests.exceptions.HTTPError as e:
                if self.fail_on_errors:
                    raise
                verbose_print(e)

    @elapsed
    def delete_all_topic_elements(self, topic):
        url = f"{self.base_url}/api/2/topics/{topic}/elements/"
        if not self.dry_run:
            headers = {'Content-Type': 'application/json', 'X-API-KEY': self.token}
            session.delete(url, headers=headers).raise_for_status()


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

    conf = {}
    for u in (universe,) + extra_configs:
        conf.update(yaml.safe_load(u.read_text()))

    url = conf['api']['url']
    token = os.getenv("DATAGOUV_API_KEY", conf['api']['token'])
    api = ApiHelper(url, token, fail_on_errors=fail_on_errors, dry_run=dry_run)

    topic_slug = conf['topic']

    grist_orgs = get_grist_orgs(conf['grist_url'], conf['env'])

    print(f"Starting at {datetime.datetime.now():%c}")
    if dry_run:
        print("*** DRY RUN ***")

    t_count: dict[ElementClass, int] = defaultdict(int)
    t_all = time.time()
    try:
        verbose_print(f"Getting existing datasets for topic '{topic_slug}'")
        orgs: list[Organization] = []

        for org in grist_orgs:
            verbose_print(f"Checking organization '{org.slug}'")
            try:
                api_org = api.get_organization(org.slug)
                orgs.append(
                    Organization(
                        id=api_org["id"],
                        name=api_org["name"],
                        slug=org.slug,
                        type=org.type,
                    )
                )
            except requests.exceptions.HTTPError:
                print(f"Unknown organization '{org.slug}'", file=sys.stderr)

        orgs = sort_orgs_by_name(orgs)

        print(f"Processing {len(orgs)} organizations...")

        if reset:
            print(f"Removing ALL elements from topic '{topic_slug}'")
            api.delete_all_topic_elements(topic_slug)

        active_orgs: dict[ElementClass, list[Organization]] = defaultdict(list)

        print(f"Processing topic '{topic_slug}'")
        for element_class in ElementClass:
            new_objects = []
            for org in orgs:
                verbose_print(f"Fetching {element_class.name} for organization '{org.slug}'...")
                objects = api.get_organization_objects(org.id, element_class)
                if not objects and not keep_empty:
                    verbose_print(f"Skipping empty organization '{org.slug}'")
                    continue
                t_count[element_class] += len(objects)
                active_orgs[element_class].append(org)
                new_objects += objects

            existing_elements = api.get_topic_elements(topic_slug, element_class)
            existing_object_ids = set(e.object_id for e in existing_elements)
            print(f"Found {len(existing_object_ids)} existing {element_class.name} in universe topic.")
            additions = list(set(new_objects) - existing_object_ids)
            removals = list(existing_object_ids - set(new_objects))
            if len(removals) > REMOVALS_THRESHOLD:
                raise Exception(f"Too many removals ({len(removals)}), aborting")
            print(f"Feeding {len(additions)} {element_class.value}...")
            for batch in batched(additions, 1000):
                api.put_topic_elements(topic_slug, element_class, batch)
            print(f"Removing {len(removals)} {element_class.value}...")
            elements_removals = [e.element_id for e in existing_elements if e.object_id in removals]
            api.delete_topic_elements(topic_slug, elements_removals)

    finally:
        print(f"Elapsed: {time.time() - t_all:.2f} s")
        for element_class in ElementClass:
            print(f"Total count {element_class.value}: {t_count[element_class]}")

    for element_class in ElementClass:
        filename = f"organizations-{element_class.value}-{conf['env']}.json"
        write_organizations_file(filename, active_orgs[element_class])

    # FIXME: remove when front uses the new file path
    # retrocompatibility
    copyfile(f"dist/organizations-datasets-{conf['env']}.json", f"dist/organizations-{conf['env']}.json")

    # Build a list of organizations from the list of bouquets
    print("Fetching organizations from bouquets...")
    bouquets = api.get_bouquets(conf["topic"])
    bouquet_orgs = list(
        {
            o["id"]: Organization(id=o["id"], name=o["name"], slug=o["slug"], type="NA")
            for b in bouquets
            if (o := b.get("organization"))
        }.values()
    )
    bouquet_orgs = sort_orgs_by_name(bouquet_orgs)
    write_organizations_file(f"organizations-bouquets-{conf['env']}.json", bouquet_orgs)

    print(f"Done at {datetime.datetime.now():%c}")


if __name__ == "__main__":
    run(feed_universe)
