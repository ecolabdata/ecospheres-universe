from ecospheres_universe.config import Config
from ecospheres_universe.feed_universe import feed

from .conftest import mock_categories, mock_feed, assert_outputs
from .datagouv_mock import DatagouvMock
from .grist_mock import GristMock
from .util import cycle_n


CATEGORIES = ["category-A", None, "category-B", "category-C"]


def test_all_at_once(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = datagouv.mock_organizations(5)
    categories = mock_categories(organizations, CATEGORIES)
    datasets = datagouv.mock_datasets(3, organizations)
    dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

    existing_universe = datagouv.mock_topic(objects=[datasets[0], datasets[1], dataservices[0]])
    upcoming_universe = datagouv.clone_topic(
        existing_universe, add=[datasets[2], dataservices[1]], remove=[datasets[1]]
    )

    bouquets = [
        datagouv.mock_topic(organization=org) for org in cycle_n(datagouv.mock_organizations(3), 5)
    ]

    mock_feed(datagouv, grist, existing_universe, upcoming_universe, bouquets, categories)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, bouquets, categories)


def test_no_changes(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = datagouv.mock_organizations(5)
    categories = mock_categories(organizations, CATEGORIES)
    datasets = datagouv.mock_datasets(3, organizations)
    dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

    existing_universe = datagouv.mock_topic(objects=[*datasets, *dataservices])
    upcoming_universe = datagouv.clone_topic(existing_universe)

    mock_feed(datagouv, grist, existing_universe, upcoming_universe, categories=categories)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, categories=categories)


def test_bootstrap_universe(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = datagouv.mock_organizations(5)
    categories = mock_categories(organizations, CATEGORIES)
    datasets = datagouv.mock_datasets(3, organizations)
    dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

    existing_universe = datagouv.mock_topic()
    upcoming_universe = datagouv.clone_topic(existing_universe, add=[*datasets, *dataservices])

    mock_feed(datagouv, grist, existing_universe, upcoming_universe, categories=categories)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, categories=categories)


def test_remove_everything(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = datagouv.mock_organizations(5)
    categories = mock_categories(organizations, CATEGORIES)
    datasets = datagouv.mock_datasets(3, organizations)
    dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

    existing_universe = datagouv.mock_topic([*datasets, *dataservices])
    upcoming_universe = datagouv.clone_topic(existing_universe, remove=[*datasets, *dataservices])

    mock_feed(datagouv, grist, existing_universe, upcoming_universe, categories=categories)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, categories=categories)


def test_bouquets_orgs(config: Config, datagouv: DatagouvMock, grist: GristMock):
    existing_universe = datagouv.mock_topic()
    upcoming_universe = datagouv.clone_topic(existing_universe)

    organizations = datagouv.mock_organizations(5)
    bouquets = [datagouv.mock_topic(organization=org) for org in organizations]

    mock_feed(datagouv, grist, existing_universe, upcoming_universe, bouquets)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, bouquets)
