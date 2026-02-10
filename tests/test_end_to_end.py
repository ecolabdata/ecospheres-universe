from typing import Any

from ecospheres_universe.config import Config
from ecospheres_universe.feed_universe import feed

from .conftest import assert_outputs, mock_feed
from .datagouv_mock import DatagouvMock
from .grist_mock import GristMock
from .util import cycle_n


from ecospheres_universe.datagouv import Organization


def test_all_at_once(config: Config, datagouv: DatagouvMock, grist: GristMock):
    organizations = [datagouv.make_organization(f"org-{i}") for i in range(5)]

    datasets = [datagouv.make_dataset(f"dataset-{i}", organization=f"org-{i}") for i in range(0, 3)]
    dataservices = [
        datagouv.make_dataservice(f"dataservice-{i}", organization=f"org-{i}") for i in range(3, 5)
    ]

    existing_universe = datagouv.make_universe(
        "existing",
        [
            *datagouv.datasets("dataset-1", "dataset-2"),
            *datagouv.dataservices("dataservice-1"),
        ],
    )

    grist_universe: list[tuple[Any, str]] = [  # FIXME: Any
        (Organization, org.id) for org in organizations
    ]

    upcoming_universe = datagouv.make_universe(
        "upcoming",
        [
            obj
            for entry in grist_universe
            for obj in datagouv.objects[entry[1]].children  # FIXME: helper
        ],
    )

    bouquet_orgs = [datagouv.make_organization(f"bouquet-org-{i}") for i in range(3)]
    bouquets = [
        datagouv.make_topic(f"bouquet-{i}", organization=org.id)
        for i, org in enumerate(cycle_n(bouquet_orgs, 5))
    ]

    mock_feed(datagouv, grist, grist_universe, existing_universe, upcoming_universe, bouquets)

    feed(config)

    assert_outputs(datagouv, upcoming_universe, bouquets)


# def test_no_changes(config: Config, datagouv: DatagouvMock, grist: GristMock):
#     organizations = datagouv.mock_organizations(5)
#     datasets = datagouv.mock_datasets(3, organizations)
#     dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

#     existing_universe = datagouv.mock_topic(objects=[*datasets, *dataservices])
#     upcoming_universe = datagouv.clone_topic(existing_universe)

#     mock_feed(datagouv, grist, existing_universe, upcoming_universe)

#     feed(config)

#     assert_outputs(datagouv, upcoming_universe)


# def test_bootstrap_universe(config: Config, datagouv: DatagouvMock, grist: GristMock):
#     organizations = datagouv.mock_organizations(5)
#     datasets = datagouv.mock_datasets(3, organizations)
#     dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

#     existing_universe = datagouv.mock_topic()
#     upcoming_universe = datagouv.clone_topic(existing_universe, add=[*datasets, *dataservices])

#     mock_feed(datagouv, grist, existing_universe, upcoming_universe)

#     feed(config)

#     assert_outputs(datagouv, upcoming_universe)


# def test_remove_everything(config: Config, datagouv: DatagouvMock, grist: GristMock):
#     organizations = datagouv.mock_organizations(5)
#     datasets = datagouv.mock_datasets(3, organizations)
#     dataservices = datagouv.mock_dataservices(2, list(reversed(organizations)))

#     existing_universe = datagouv.mock_topic([*datasets, *dataservices])
#     upcoming_universe = datagouv.clone_topic(existing_universe, remove=[*datasets, *dataservices])

#     mock_feed(datagouv, grist, existing_universe, upcoming_universe)

#     feed(config)

#     assert_outputs(datagouv, upcoming_universe)


# def test_bouquets_orgs(config: Config, datagouv: DatagouvMock, grist: GristMock):
#     existing_universe = datagouv.mock_topic()
#     upcoming_universe = datagouv.clone_topic(existing_universe)

#     organizations = datagouv.mock_organizations(5)
#     bouquets = [datagouv.mock_topic(organization=org) for org in organizations]

#     mock_feed(datagouv, grist, existing_universe, upcoming_universe, bouquets)

#     feed(config)

#     assert_outputs(datagouv, upcoming_universe, bouquets)
