from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import Organization
from ecospheres_universe.feed_universe import feed

from .conftest import assert_outputs
from .datagouv_mock import DatagouvMock
from .grist_mock import GristMock
from .util import cycle_n


def test_all_at_once(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:3]]
    dataservices = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = datasets[:2] + dataservices[:1]
    grist_universe = [grist.entry(org, category=f"cat-{org.id}") for org in organizations]

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
    grist_universe = [grist.entry(org) for org in organizations]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_bootstrap_universe(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    _ = [datagouv.dataset(organization=org) for org in organizations[:3]]
    _ = [datagouv.dataservice(organization=org) for org in organizations[3:]]

    existing_universe = []
    grist_universe = [grist.entry(org) for org in organizations]

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


def test_duplicate_element(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(2)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:2]]

    existing_universe = [datasets[0], datasets[1], datasets[0]]  # duplicate datasets[0]
    grist_universe = [grist.entry(org) for org in organizations]

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


def test_unknown_entry(config: Config, datagouv: DatagouvMock, grist: GristMock):
    existing_universe = []
    grist_universe = [grist.raw_entry("unknown", Organization)]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_datasets_dataservices_entries(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(4)]
    datasets = [datagouv.dataset(organization=org) for org in organizations[:2]]
    dataservices = [datagouv.dataservice(organization=org) for org in organizations[2:]]

    existing_universe = datasets + dataservices
    grist_universe = [grist.entry(datasets[0]), grist.entry(dataservices[0])]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_topic_entries(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    topics = [datagouv.topic(organization=organizations[i]) for i in range(3)]
    datasets = [
        datagouv.dataset(organization=organizations[0], topics=topics[0:1]),
        datagouv.dataset(organization=organizations[1], topics=topics[1:2]),
        datagouv.dataset(organization=organizations[2], topics=[topics[2]]),
    ]
    dataservices = [
        datagouv.dataservice(organization=organizations[3], topics=[topics[0]]),
        datagouv.dataservice(organization=organizations[4], topics=[topics[2]]),
    ]

    existing_universe = datasets + dataservices
    grist_universe = [grist.entry(topics[0]), grist.entry(topics[1])]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)


def test_tag_entries(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.organization() for _ in range(5)]
    tags = [datagouv.tag(organization=organizations[i]) for i in range(3)]
    datasets = [
        datagouv.dataset(organization=organizations[0], tags=tags[0:1]),
        datagouv.dataset(organization=organizations[1], tags=tags[1:2]),
        datagouv.dataset(organization=organizations[2], tags=[tags[2]]),
    ]
    dataservices = [
        datagouv.dataservice(organization=organizations[3], tags=[tags[0]]),
        datagouv.dataservice(organization=organizations[4], tags=[tags[2]]),
    ]

    existing_universe = datasets + dataservices
    grist_universe = [grist.entry(tags[0]), grist.entry(tags[1])]

    grist.mock(grist_universe)
    datagouv.mock(existing_universe, grist_universe)

    feed(config)

    assert_outputs(datagouv, grist_universe)
