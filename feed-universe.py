import argparse
import datetime
import functools
import json
import os
import sys
import time

from typing import NamedTuple

import requests
import yaml

session = requests.Session()

# noop unless args.verbose is set
verbose = lambda *a, **k: None


class Organization(NamedTuple):
    id: str
    name: str
    slug: str
    type: str


class GristOrganization(NamedTuple):
    slug: str
    type: str


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
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def get_grist_orgs(grist_url: str, env: str) -> list[GristOrganization]:
    r = requests.get(grist_url, params={'filter': json.dumps({'env': [env]}), 'limit': 0})
    r.raise_for_status()
    return [
        GristOrganization(slug=o['fields']['slug'], type=o['fields']['type'])
        for o in r.json()['records']
    ]


class IndentedDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentedDumper, self).increase_indent(flow, False)


class ApiHelper:

    def __init__(self, base_url, token, fail_on_errors=False, dry_run=False):
        self.base_url = base_url
        self.token = token
        self.fail_on_errors = fail_on_errors
        self.dry_run = dry_run

    @elapsed_and_count
    def get_datasets(self, url, func, xfields='data{id}'):
        datasets = set()
        try:
            headers={'X-Fields': f"{xfields},next_page"}
            while True:
                r = session.get(url, headers=headers)
                r.raise_for_status()
                c = r.json()
                datasets |= func(c)
                url = c.get('next_page')
                if not url:
                    break
        except requests.exceptions.HTTPError as e:
            if self.fail_on_errors:
                raise
            verbose(e)
        return list(datasets)

    def get_organization(self, org: str):
        url = f"{self.base_url}/api/1/organizations/{org}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()

    def get_topic_id(self, topic_slug: str):
        url = f"{self.base_url}/api/2/topics/{topic_slug}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()['id']

    @elapsed_and_count
    def get_organizations(self, query):
        orgs = set()
        url = f"{self.base_url}/api/1/organizations/?q={query}&page_size=1000"
        headers = {'X-Fields': 'data{slug},next_page'}
        while True:
            r = session.get(url, headers=headers)
            r.raise_for_status()
            c = r.json()
            orgs |= {d['slug'] for d in c['data']}
            url = c['next_page']
            if not url:
                break
        return list(orgs)

    def get_organization_datasets(self, org: str):
        url = f"{self.base_url}/api/1/organizations/{org}/datasets/?page_size=1000"
        xfields = 'data{id,archived,deleted,private,extras{geop:dataset_id}}'
        func = lambda c: {d['id'] for d in c['data']
                          if not bool(d.get('archived') or d.get('deleted')
                                      or d.get('private') or d.get('extras'))}
        return self.get_datasets(url, func, xfields=xfields)

    def get_topic_datasets(self, topic, organization_id: str = ""):
        url = f"{self.base_url}/api/2/topics/{topic}/datasets/?page_size=1000"
        if organization_id:
            url += f"&organization={organization_id}"
        func = lambda c: {d['id'] for d in c['data']}
        return self.get_datasets(url, func)

    def get_topic_datasets_v1(self, topic):
        url = f"{self.base_url}/api/1/topics/{topic}/"
        xfields = 'datasets{id}'
        func = lambda c: {d['id'] for d in c['datasets']}
        return self.get_datasets(url, func, xfields=xfields)

    def search_datasets_count(self, topic_id: str | None, organization_id: str = ""):
        if not topic_id:
            return
        url = f"{self.base_url}/api/2/datasets/search/?topic={topic_id}"
        if organization_id:
            url += f"&organization={organization_id}"
        r = session.get(url)
        r.raise_for_status()
        return r.json()['total']

    @elapsed_and_count
    def put_topic_datasets(self, topic, datasets):
        url = f"{self.base_url}/api/2/topics/{topic}/datasets/"
        headers = {'Content-Type': 'application/json', 'X-API-KEY': self.token}
        data = json.dumps([{'id': d} for d in datasets])
        if not self.dry_run:
            session.post(url, data=data, headers=headers).raise_for_status()
        return datasets

    @elapsed
    def delete_topic_datasets(self, topic, datasets):
        for d in datasets:
            try:
                url = f"{self.base_url}/api/2/topics/{topic}/datasets/{d}/"
                headers = {'X-API-KEY': self.token}
                if not self.dry_run:
                    session.delete(url, headers=headers).raise_for_status()
            except requests.exceptions.HTTPError as e:
                if self.fail_on_errors:
                    raise
                verbose(e)

    #FIXME: doesn't work on huge topics
    @elapsed
    def delete_all_topic_datasets(self, topic):
        # workaround for missing "delete all" endpoint
        # 1. get needed info from topic
        url = f"{self.base_url}/api/2/topics/{topic}/datasets/?page_size=1"
        r = session.get(url, headers={'X-Fields': 'data{id}'})
        r.raise_for_status()
        c = r.json()
        single = [d['id'] for d in c.get('data', [])] # page_size=1

        url = f"{self.base_url}/api/2/topics/{topic}/"
        r = session.get(url, headers={'X-Fields': 'tags'})
        r.raise_for_status()
        c = r.json()
        tags = c.get('tags', [])

        # 2. override (api v1) existing datasets with list of 1 element
        url = f"{self.base_url}/api/1/topics/{topic}/"
        data = json.dumps({'datasets': single, 'tags': tags})
        headers = {'Content-Type': 'application/json', 'X-API-KEY': self.token}
        if not self.dry_run:
            session.put(url, data=data, headers=headers).raise_for_status()

        # 3. delete element from step 2
        if not self.dry_run:
            self.delete_topic_datasets(topic, single)

    #FIXME: doesn't work on huge topics
    @elapsed
    def slow_delete_all_topic_datasets(self, topic):
        print("Delete v2")
        datasets = self.get_topic_datasets(topic)
        self.delete_topic_datasets(topic, datasets)
        print("Delete hack")
        try:
            api.delete_all_topic_datasets(topic)
        except:
            pass
        print("Delete v1")
        datasets = self.get_topic_datasets_v1(topic)
        self.delete_topic_datasets(topic, datasets)


def check_sync(org: Organization, datasets: list):
    topic_datasets = api.get_topic_datasets(topic, organization_id=org.id)
    datasets_search_count = api.search_datasets_count(topic_id, organization_id=org.id)
    if not(len(topic_datasets) == len(datasets) == datasets_search_count):
        print(f"Datasets for '{org.slug}' are NOT in sync", file=sys.stderr)
        print(f"  - topic datasets : {len(topic_datasets)}", file=sys.stderr)
        print(f"  - universe       : {len(datasets)}", file=sys.stderr)
        print(f"  - search datasets: {datasets_search_count}", file=sys.stderr)
    else:
        verbose(f"Datasets for '{org.slug}' are in sync ({len(datasets)} datasets)")


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
    parser.add_argument('-s', '--slow', action='store_true', default=False,
                        help='enable slow reset mode')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='enable verbose mode')
    parser.add_argument('-c', '--check', action='store_true', default=False,
                        help='only check synchronization status')
    args = parser.parse_args()

    conf = {}
    for u in args.universe:
        conf.update(yaml.safe_load(u))

    url = conf['api']['url']
    token = os.getenv("DATAGOUV_API_KEY", conf['api']['token'])
    api = ApiHelper(url, token, fail_on_errors=args.fail_on_errors, dry_run=args.dry_run)

    topic = conf['topic']
    topic_id = None

    grist_orgs = get_grist_orgs(conf['grist_url'], conf['env'])
    grist_orgs = sorted(grist_orgs, key=lambda o: o.slug)

    if args.verbose:
        verbose = print

    print(f"Starting at {datetime.datetime.now():%c}")
    if args.dry_run:
        print("*** DRY RUN ***")
    elif args.check:
        print("*** CHECKING ***")
        topic_id = api.get_topic_id(topic)

    t_count = 0
    t_all = time.time()
    try:
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
                print(f"Unknown organization '{org.slug}'")

        print(f"Processing {len(orgs)} organizations...")

        if args.reset:
            print(f"Removing ALL datasets from topic '{topic}'")
            if args.slow:
                api.slow_delete_all_topic_datasets(topic)
            else:
                api.delete_all_topic_datasets(topic)

        print(f"Processing topic '{topic}'")
        active_orgs: list[Organization] = []
        for org in orgs:
            verbose(f"Fetching datasets for organization '{org.slug}'...")
            datasets = api.get_organization_datasets(org.slug)
            if not datasets and not args.keep_empty:
                verbose(f"Skipping empty organization '{org.slug}'")
                continue

            t_count += len(datasets)
            active_orgs.append(org)

            if args.check:
                check_sync(org, datasets)
                continue

            print(f"Feeding {len(datasets)} datasets from '{org.slug}'...")
            for batch in batched(datasets, 1000):
                api.put_topic_datasets(topic, batch)

    finally:
        print(f"Total count: {t_count}, elapsed: {time.time() - t_all:.2f} s")

    if not args.check:
        filename = f"organizations-{conf['env']}.json"
        print(f"Generating output file {filename}...")
        with open(f"dist/{filename}", "w") as f:
            json.dump([o._asdict() for o in active_orgs], f, indent=2 if args.verbose else None)

    print(f"Done at {datetime.datetime.now():%c}")
