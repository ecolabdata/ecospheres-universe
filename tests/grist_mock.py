from dataclasses import dataclass

from responses import RequestsMock
from responses.matchers import query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvObject


@dataclass
class GristEntry:
    type: type[DatagouvObject]
    identifier: str


class GristMock:
    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def mock(self, universe: list[GristEntry]) -> None:
        self.mock_get_entries(universe)

    def mock_get_entries(self, universe: list[GristEntry]) -> None:
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {"fields": {"Type": entry.type.object_name(), "Identifiant": entry.identifier}}
                    for entry in universe
                ]
            },
        )
