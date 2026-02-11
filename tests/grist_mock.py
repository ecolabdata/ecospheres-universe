from responses import RequestsMock
from responses.matchers import query_param_matcher

from ecospheres_universe.config import Config
from ecospheres_universe.datagouv import DatagouvObject
from ecospheres_universe.grist import GristAction, GristEntry


class GristMock:
    responses: RequestsMock
    config: Config

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def entry(
        self, object: DatagouvObject, exclude: bool = False, category: str | None = None
    ) -> GristEntry:
        return GristEntry(object.id, type(object), exclude, category)

    def raw_entry(
        self,
        identifier: str,
        object_class: type[DatagouvObject],
        exclude: bool = False,
        category: str | None = None,
    ) -> GristEntry:
        return GristEntry(identifier, object_class, exclude, category)

    def mock(self, universe: list[GristEntry]) -> None:
        # grist.get_entries()
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0})],
            json={
                "records": [
                    {
                        "fields": {
                            "Type": entry.object_class.model_name(),
                            "Identifiant": entry.identifier,
                            "Action": GristAction.EXCLURE if entry.exclude else GristAction.INCLURE,
                            "Categorie": entry.category,
                        }
                    }
                    for entry in universe
                ]
            },
        )
