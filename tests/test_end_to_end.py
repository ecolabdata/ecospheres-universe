from typing import Callable

from ecospheres_universe.config import Config

from .datagouv_mocks import Dataset, Dataservice, Organization, Topic


def test_all_at_once(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)
    datasets = Dataset.some(3, organizations)
    dataservices = Dataservice.some(2, list(reversed(organizations)))

    existing_universe = Topic(datasets[0], datasets[1], dataservices[0])
    upcoming_universe = (
        existing_universe.clone()
        .add_elements(datasets[2], dataservices[1])
        .remove_elements(datasets[1])
    )

    bouquets = Topic.some(5, [org.with_type(None) for org in Organization.some(n=3)])

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets)


def test_no_changes(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)
    datasets = Dataset.some(3, organizations)
    dataservices = Dataservice.some(2, list(reversed(organizations)))

    existing_universe = Topic(*datasets, *dataservices)
    upcoming_universe = existing_universe.clone()

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_bootstrap_universe(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)
    datasets = Dataset.some(3, organizations)
    dataservices = Dataservice.some(2, list(reversed(organizations)))

    existing_universe = Topic()
    upcoming_universe = existing_universe.clone().add_elements(*datasets, *dataservices)

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_remove_everything(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)
    datasets = Dataset.some(3, organizations)
    dataservices = Dataservice.some(2, list(reversed(organizations)))

    existing_universe = Topic(*datasets, *dataservices)
    upcoming_universe = existing_universe.clone().remove_elements(*datasets, *dataservices)

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_duplicate_element(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)
    datasets = Dataset.some(3, organizations)

    existing_universe = Topic(datasets[0], datasets[1], datasets[0])  # duplicate datasets[0]
    upcoming_universe = (
        existing_universe.clone()
        # ensure we only have a single occurrence of datasets[0]
        .remove_elements(datasets[0])
        .add_elements(datasets[0])
    )

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_bouquets_orgs(run_mock_feed: Callable, feed_config: Config):
    existing_universe = Topic()
    upcoming_universe = existing_universe.clone()

    organizations = [org.with_type(None) for org in Organization.some(5)]
    bouquets = Topic.some(len(organizations), organizations)

    run_mock_feed(feed_config, existing_universe, upcoming_universe, bouquets)
