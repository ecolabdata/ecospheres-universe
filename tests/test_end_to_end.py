from typing import Callable

from ecospheres_universe.config import Config

from .datagouv_mocks import Dataset, Dataservice, Organization, Topic


def test_all_at_once(mock_feed_and_assert: Callable, feed_config: Config):
    organizations = Organization.many(5)
    datasets = Dataset.many(3, organizations)
    dataservices = Dataservice.many(2, list(reversed(organizations)))

    existing_universe = Topic(datasets[0], datasets[1], dataservices[0])
    upcoming_universe = (
        existing_universe.clone()
        .add_elements(datasets[2], dataservices[1])
        .remove_elements(datasets[1])
    )

    bouquets = Topic.many(5, [org.with_category(None) for org in Organization.many(n=3)])

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets)


def test_no_changes(mock_feed_and_assert: Callable, feed_config: Config):
    organizations = Organization.many(5)
    datasets = Dataset.many(3, organizations)
    dataservices = Dataservice.many(2, list(reversed(organizations)))

    existing_universe = Topic(*datasets, *dataservices)
    upcoming_universe = existing_universe.clone()

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_bootstrap_universe(mock_feed_and_assert: Callable, feed_config: Config):
    organizations = Organization.many(5)
    datasets = Dataset.many(3, organizations)
    dataservices = Dataservice.many(2, list(reversed(organizations)))

    existing_universe = Topic()
    upcoming_universe = existing_universe.clone().add_elements(*datasets, *dataservices)

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_remove_everything(mock_feed_and_assert: Callable, feed_config: Config):
    organizations = Organization.many(5)
    datasets = Dataset.many(3, organizations)
    dataservices = Dataservice.many(2, list(reversed(organizations)))

    existing_universe = Topic(*datasets, *dataservices)
    upcoming_universe = existing_universe.clone().remove_elements(*datasets, *dataservices)

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_duplicate_element(mock_feed_and_assert: Callable, feed_config: Config):
    organizations = Organization.many(5)
    datasets = Dataset.many(3, organizations)

    existing_universe = Topic(datasets[0], datasets[1], datasets[0])  # duplicate datasets[0]
    upcoming_universe = (
        existing_universe.clone()
        # ensure we only have a single occurrence of datasets[0]
        .remove_elements(datasets[0])
        .add_elements(datasets[0])
    )

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets=None)


def test_bouquets_orgs(mock_feed_and_assert: Callable, feed_config: Config):
    existing_universe = Topic()
    upcoming_universe = existing_universe.clone()

    organizations = [org.with_category(None) for org in Organization.many(5)]
    bouquets = Topic.many(len(organizations), organizations)

    mock_feed_and_assert(feed_config, existing_universe, upcoming_universe, bouquets)
