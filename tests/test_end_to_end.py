from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import Organization
from ecospheres_universe.feed_universe import feed

from .conftest import assert_outputs
from .datagouv_mock import DatagouvMock
from .grist_mock import GristEntry, GristMock
from .util import cycle_n


def test_all_at_once(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:3]]
    dataservices = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = datasets[:2] + dataservices[:1]
    grist_universe = [GristEntry(Organization, org.id, f"cat-{org.id}") for org in organizations]

    bouquet_orgs = [datagouv.organization() for _ in range(3)]
    bouquets = [datagouv.topic(organization=org) for org in cycle_n(bouquet_orgs, 5)]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe, bouquets)

    feed(config)

    assert_outputs(datagouv, grist_universe, bouquets)


def test_no_changes(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:3]]
    dataservices = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = datasets + dataservices
    grist_universe = [GristEntry(Organization, org.id) for org in organizations]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_bootstrap_universe(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    _ = [datagouv.dataset(organization=org) for org in organizations[:3]]
    _ = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = []
    grist_universe = [GristEntry(Organization, org.id) for org in organizations]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_remove_everything(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:3]]
    dataservices = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = datasets + dataservices
    grist_universe = []

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_bouquets_orgs(config: Config, datagouv: DatagouvMock, grist: GristMock):
    existing_universe = []
    grist_universe = []

    organizations = [datagouv.organization() for _ in range(5)]
    bouquets = [datagouv.topic(organization=org) for org in organizations]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe, bouquets)

    feed(config)

    assert_outputs(datagouv, grist_universe, bouquets)
