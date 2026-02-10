from responses import RequestsMock
from responses.matchers import query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvObject


class GristMock:
    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def mock_get_entries(self, universe: list[tuple[DatagouvObject, str]]) -> None:
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {"fields": {"Type": entry[0].object_name(), "Identifiant": entry[1]}}
                    for entry in universe
                ]
            },
        )
