from typing import Callable

from ecospheres_universe.config import Config

from .datagouv_mocks import Dataset, Dataservice, Organization, Topic


def test_full_run(run_mock_feed: Callable, feed_config: Config):
    organizations = Organization.some(5)

    datasets = Dataset.some_owned(3, organizations)
    dataservices = Dataservice.some_owned(2, list(reversed(organizations)))

    existing_universe = Topic(datasets[0], datasets[1], dataservices[0])

    target_universe = (
        existing_universe.clone()
        .add_elements(datasets[2], dataservices[1])
        .remove_elements(datasets[1])
    )

    bouquets = Topic.some_owned(5, [org.with_type(None) for org in Organization.some(n=3)])

    run_mock_feed(feed_config, existing_universe, target_universe, bouquets)
