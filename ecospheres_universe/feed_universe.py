import argparse
import datetime
import functools
import json
import os
import sys
import time

from enum import Enum
from shutil import copyfile
from typing import NamedTuple

import requests
import yaml

REMOVALS_THRESHOLD = 1800

session = requests.Session()

# noop unless args.verbose is set
def verbose(*args, **kwargs):
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
            verbose(f"<{func.__name__}: count={len(val or [])}, elapsed={time.time() - t:.2f}s>")
        return val
    return wrapper_decorator

def elapsed(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        try:
            val = func(*args, **kwargs)
        finally:
            verbose(f"<{func.__name__}: elapsed={time.time() - t:.2f}s>")
        return val
    return wrapper_decorator


def batched(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]


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
            verbose(e)
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

    def get_organization_objects(self, org_id: str, element_class: ElementClass) -> list[str]:
        url = f"{self.base_url}/api/2/{element_class.value}/search/?organization={org_id}&page_size=1000"
        xfields = 'data{id,archived,deleted,private,extras{geop:dataset_id}}'
        def filter_objects(c):
            return {
                d["id"]
                for d in c["data"]
                if not bool(
                    d.get("archived")
                    or d.get("deleted")
                    or d.get("private")
                    or d.get("extras")
                )
            }
        return self.get_objects(url, filter_objects, xfields=xfields)

    def get_organization_datasets(self, org: str):
        # Kept for backward compatibility
        api_org = self.get_organization(org)
        return self.get_organization_objects(api_org["id"], ElementClass.Dataset)

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
                verbose(e)

    @elapsed
    def delete_all_topic_elements(self, topic):
        url = f"{self.base_url}/api/2/topics/{topic}/elements/"
        if not self.dry_run:
            headers = {'Content-Type': 'application/json', 'X-API-KEY': self.token}
            session.delete(url, headers=headers).raise_for_status()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('universe', nargs='+', type=argparse.FileType('r'), metavar='config',
                        help='universe yaml config file')
    parser.add_argument('-e', '--keep-empty', action='store_true', default=False,
                        help='keep empty organizations in the list')
    parser.add_argument('-f', '--fail-on-errors', action='store_true', default=False,
                        help='fail the run on http errors')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help='perform a trial run without actual feeding')
    parser.add_argument('-r', '--reset', action='store_true', default=False,
                        help='empty topic before refeeding it')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='enable verbose mode')
    args = parser.parse_args()

    conf = {}
    for u in args.universe:
        conf.update(yaml.safe_load(u))

    url = conf['api']['url']
    token = os.getenv("DATAGOUV_API_KEY", conf['api']['token'])
    api = ApiHelper(url, token, fail_on_errors=args.fail_on_errors, dry_run=args.dry_run)

    topic_slug = conf['topic']

    grist_orgs = get_grist_orgs(conf['grist_url'], conf['env'])

    if args.verbose:
        verbose = print

    print(f"Starting at {datetime.datetime.now():%c}")
    if args.dry_run:
        print("*** DRY RUN ***")

    t_count = {element_class: 0 for element_class in ElementClass}
    t_all = time.time()
    try:
        verbose(f"Getting existing datasets for topic '{topic_slug}'")
        orgs: list[Organization] = []

        for org in grist_orgs:
            verbose(f"Checking organization '{org.slug}'")
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

        print(f"Processing {len(orgs)} organizations...")

        if args.reset:
            print(f"Removing ALL elements from topic '{topic_slug}'")
            api.delete_all_topic_elements(topic_slug)

        active_orgs: dict[ElementClass, set[Organization]] = {element_class: set() for element_class in ElementClass}

        print(f"Processing topic '{topic_slug}'")
        for element_class in ElementClass:
            new_objects = []
            for org in orgs:
                verbose(f"Fetching {element_class.name} for organization '{org.slug}'...")
                objects = api.get_organization_objects(org.id, element_class)
                if not objects and not args.keep_empty:
                    verbose(f"Skipping empty organization '{org.slug}'")
                    continue
                t_count[element_class] += len(objects)
                active_orgs[element_class].add(org)
                new_objects += objects

            existing_elements = api.get_topic_elements(topic_slug, element_class)
            additions = list(set(new_objects) - set(e.object_id for e in existing_elements))
            removals = list(set(e.object_id for e in existing_elements) - set(new_objects))
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
        print(f"Generating output file {filename}...")
        with open(f"dist/{filename}", "w") as f:
            json.dump([o._asdict() for o in active_orgs[element_class]], f, indent=2, ensure_ascii=False)

    # retrocompatibility
    copyfile(f"dist/organizations-datasets-{conf['env']}.json", f"dist/organizations-{conf['env']}.json")

    print(f"Done at {datetime.datetime.now():%c}")
