from ecospheres_universe.config import ApiConfig, Config, DeployEnv

from .datagouv_mocks import Dataset, Dataservice, Organization, Topic


def test_full_run(mock_feed, tmp_path):
    config = Config(
        env=DeployEnv.DEMO,
        topic="test-topic",
        tag="test-tag",
        grist_url="https://www.example.com/grist",
        output_dir=tmp_path,
        api=ApiConfig(
            url="https://www.example.com/datagouv",
            token="fake-token",
        ),
    )

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

    mock_feed(config, existing_universe, target_universe, bouquets)
