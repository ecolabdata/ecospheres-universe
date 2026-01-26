from copy import copy
from itertools import cycle, islice
from typing import final, override, ClassVar, Self

from ecospheres_universe.datagouv import (
    DatagouvObject,
    Dataservice,
    Dataset,
    Organization,
    Topic,
    TopicElement,
    TopicObject,
)
from ecospheres_universe.util import JSONObject, uniquify


class MockObject(DatagouvObject):
    _id_counter: ClassVar[int] = 0
    _id: int

    def __init__(self):
        self._id_counter += 1
        self._id = self._id_counter

    def __repr__(self) -> str:
        return f"<{self.id}>"

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def id(self) -> str:
        return f"{self.__class__.__name__.lower()}-{self._id}"

    @property
    def slug(self) -> str:
        return f"{self.__class__.__name__.lower()}-slug-{self._id}"

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__} {self._id}"

    def as_json(self) -> JSONObject:
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
        }


@final
class MockOrganization(MockObject, Organization):
    _CATEGORIES: ClassVar[list[str | None]] = ["category-A", None, "category-B", "category-C"]
    _objects: list["MockOwnedObject"]
    _category: str | None

    def __init__(self, objects: list["MockOwnedObject"] | None = None):
        super().__init__()
        self._objects = objects if objects else []
        self._category = self._CATEGORIES[self._id % len(self._CATEGORIES)]

    def __repr__(self) -> str:
        return f"<{self.id} {[o.id for o in self._objects]}>"

    @property
    def category(self) -> str | None:
        return self._category

    # FIXME: DatagouvObject is too wide
    def objects(self, object_class: type[DatagouvObject] | None = None) -> list["MockOwnedObject"]:
        return (
            [o for o in self._objects if isinstance(o, object_class)]
            if object_class
            else self._objects
        )

    @override
    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "category": self._category,
        }

    def with_category(self, category: str | None) -> Self:
        self._category = category
        return self

    def add_objects(self, *objects: "MockOwnedObject") -> Self:
        self._objects += objects
        return self

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def many(cls, n: int) -> list[Self]:
        return [cls() for _ in range(n)]


class MockOwnedObject(MockObject):
    _organization: MockOrganization

    def __repr__(self) -> str:
        return f"<{self.id} @{self._organization.id}>"

    @property
    def organization(self) -> MockOrganization:
        return self._organization

    @override
    def as_json(self) -> JSONObject:
        return {
            **super().as_json(),
            "organization": self.organization.as_json(),
        }

    def with_ownership(self, organization: MockOrganization) -> Self:
        self._organization = organization
        organization.add_objects(self)
        return self

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def many(cls, n: int, organizations: list[MockOrganization]) -> list[Self]:
        return [cls().with_ownership(org) for org in islice(cycle(organizations), n)]


@final
class MockDataset(MockOwnedObject, Dataset):
    pass


@final
class MockDataservice(MockOwnedObject, Dataservice):
    pass


@final
class MockElement(MockObject, TopicElement):
    _object: MockOwnedObject

    def __init__(self, object: MockOwnedObject):
        super().__init__()
        self._object = object

    def __repr__(self) -> str:
        return f"<{self.id} [{self._object.id}]>"

    @property
    def object(self) -> MockOwnedObject:
        return self._object


@final
class MockTopic(MockOwnedObject, Topic):
    _elements: list[MockElement]

    def __init__(self, *elements: MockOwnedObject):
        super().__init__()
        self._elements = [MockElement(e) for e in elements] if elements else []

    def __repr__(self) -> str:
        return f"<{self.id} {self._elements}>"

    def elements(self, object_class: type[TopicObject] | None = None) -> list[MockElement]:
        elements = self._elements
        if object_class:
            elements = filter(lambda e: type(e.object) is object_class, elements)
        return list(elements)

    def organizations(
        self, object_class: type[TopicObject] | None = None
    ) -> list[MockOrganization]:
        return uniquify(e.object.organization for e in self.elements(object_class))

    def clone(self) -> Self:
        clone = copy(self)
        clone._elements = [e for e in self._elements]
        return clone

    def add_elements(self, *elements: MockOwnedObject) -> Self:
        self._elements += [MockElement(e) for e in elements]
        return self

    def remove_elements(self, *elements: MockOwnedObject) -> Self:
        self._elements = [e for e in self._elements if e.object not in elements]
        return self
