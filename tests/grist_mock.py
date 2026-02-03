from responses import RequestsMock
from responses.matchers import query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import Organization, Topic

from .datagouv_mock import DatagouvMock


class GristMock:
    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def mock_get_entries(self, universe: Topic) -> None:
        orgs = DatagouvMock.get_organizations(universe.objects())
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {"fields": {"Type": Organization.object_name(), "Identifiant": org.slug}}
                    for org in orgs
                ]
            },
        )
