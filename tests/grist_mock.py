from responses import RequestsMock
from responses.matchers import query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.grist import GristEntry


class GristMock:
    responses: RequestsMock
    config: Config

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def mock(self, universe: list[GristEntry]) -> None:
        # grist.get_entries()
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {
                        "fields": {
                            "Type": entry.object_class.object_name(),
                            "Identifiant": entry.identifier,
                            "Categorie": entry.category,
                        }
                    }
                    for entry in universe
                ]
            },
        )
