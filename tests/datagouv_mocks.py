import builtins

from abc import ABC
from copy import copy
from dataclasses import dataclass, field, InitVar
from itertools import cycle, islice
from typing import override, ClassVar, Self, TypeAlias

from ecospheres_universe.datagouv import (
    DatagouvObject,
    Dataservice,
    Dataset,
    Organization,
    OwnedObject,
    Topic,
    TopicElement,
    TopicObject,
)
from ecospheres_universe.util import uniquify


# TODO: check what gets output through as_json, asdict, repr, ... and fix or document
# TODO: clarify/uniformize init=True/False
# TODO: try to change Mock* to protocols, so we don't have weird casts between Mock* and *
#       eg mock_organizations_file(uniquify(MockOrganization(**b.organization) for b in bouquets))
#       possible??? worth it??? only needed for the example above so far
#       or see idea in mock_organizations_file

# IMPORTANT: Mock* must come first in the parents list so fields are properly initialized


@dataclass
class MockDatagouvObject(DatagouvObject, ABC):
    _ID_COUNTER: ClassVar[int] = 0

    _id: int = field(default_factory=lambda: MockDatagouvObject._next_id())
    id: str = field(default_factory=lambda: MockDatagouvObject._mock_id())
    slug: str = field(default_factory=lambda: MockDatagouvObject._mock_slug())

    @classmethod
    def _next_id(cls) -> int:
        cls._ID_COUNTER += 1
        return cls._ID_COUNTER

    @classmethod
    def _mock_id(cls) -> str:
        return f"{cls.__name__}-{cls._ID_COUNTER}"

    @classmethod
    def _mock_slug(cls) -> str:
        return f"{cls.__name__.lower()}-{cls._ID_COUNTER}"

    @classmethod
    def _mock_name(cls) -> str:
        return f"{cls.__name__} {cls._ID_COUNTER}"

    @override
    def __repr__(self) -> str:
        return f"<{self.id}>"


@dataclass
class MockOrganization(MockDatagouvObject, Organization):
    _CATEGORIES: ClassVar[list[str | None]] = ["category-A", None, "category-B", "category-C"]

    name: str = field(default_factory=lambda: MockDatagouvObject._mock_name())
    type: str | None = field(default=None)  # shortcut to avoid having to mock UniverseOrg as well
    _objects: list["MockOwnedObject"] = field(init=False)

    mock_objects: InitVar[list["MockOwnedObject"] | None] = None

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def many(cls, n: int) -> list[Self]:
        return [cls() for _ in range(n)]

    # TODO: rename
    @classmethod
    def from_(cls, org: Organization) -> Self:
        return cls(id=org.id, slug=org.slug, name=org.name)

    def __post_init__(self, mock_objects: list["MockOwnedObject"] | None = None):
        self.type = self._CATEGORIES[self._id % len(self._CATEGORIES)]
        self._objects = mock_objects if mock_objects else []

    @override
    def __repr__(self) -> str:
        return f"<{self.id} {[o.id for o in self._objects]}>"

    # FIXME: why is this needed here to avoid unhashable type?
    def __hash__(self) -> int:
        return hash(self.id)

    def objects(
        self, object_class: builtins.type[OwnedObject] | None = None
    ) -> list["MockOwnedObject"]:
        return (
            [o for o in self._objects if isinstance(o, object_class)]
            if object_class
            else self._objects
        )

    def with_type(self, type: str | None) -> Self:
        self.type = type
        print(self.as_json())
        return self

    def add_objects(self, *objects: "MockOwnedObject") -> Self:
        self._objects += objects
        return self


@dataclass
class MockOwnedObject(MockDatagouvObject, OwnedObject, ABC):
    organization: Organization | None = None

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def many(cls, n: int, organizations: list[MockOrganization]) -> list[Self]:
        return [cls().with_ownership(org) for org in islice(cycle(organizations), n)]

    @override
    def __repr__(self) -> str:
        return f"<{self.id} @{self.organization.id if self.organization else 'noone'}>"

    def with_ownership(self, organization: MockOrganization) -> Self:
        self.organization = organization
        organization.add_objects(self)
        return self


MockTopicObject: TypeAlias = "MockDataset | MockDataservice"


@dataclass
class MockTopicElement(TopicElement):
    # TODO: either keep init=False, or use one/many for consistency?
    id: str = field(init=False)
    object_id: str = field(init=False)
    mock_object: MockTopicObject

    def __post_init__(self):
        self.id = self.mock_object.id  # FIXME: use something else?
        self.object_id = self.mock_object.id

    @override
    def __repr__(self) -> str:
        return f"<{self.id} [{self.mock_object.id}]>"

    @property
    def object(self) -> MockTopicObject:
        return self.mock_object


@dataclass
class MockTopic(MockOwnedObject, Topic):
    name: str = field(default_factory=lambda: MockDatagouvObject._mock_name())
    _elements: list[MockTopicElement] = field(init=False)

    # default value unused but required to pass dataclass constraints
    mock_elements: InitVar[list[MockTopicObject]] = []

    def __post_init__(self, mock_elements: list[MockTopicObject]):
        self._elements = [MockTopicElement(e) for e in mock_elements] if mock_elements else []

    @override
    def __repr__(self) -> str:
        return f"<{self.id} {self._elements}>"

    def elements(self, object_class: type[TopicObject] | None = None) -> list[MockTopicElement]:
        elements = self._elements
        if object_class:
            elements = filter(lambda e: type(e.mock_object) is object_class, elements)
        return list(elements)

    def organizations(
        self, object_class: type[TopicObject] | None = None
    ) -> list[MockOrganization]:
        return uniquify(
            MockOrganization.from_(org)
            for e in self.elements(object_class)
            if (org := e.object.organization)
        )

    def add_elements(self, *elements: MockTopicObject) -> Self:
        self._elements += [MockTopicElement(e) for e in elements]
        return self

    def remove_elements(self, *elements: MockTopicObject) -> Self:
        self._elements = [e for e in self._elements if e.mock_object not in elements]
        return self

    def clone(self) -> Self:
        clone = copy(self)
        clone._elements = [e for e in self._elements]
        return clone


@dataclass
class MockDataset(MockOwnedObject, Dataset):
    title: str = field(default_factory=lambda: MockDatagouvObject._mock_name())


@dataclass
class MockDataservice(MockOwnedObject, Dataservice):
    title: str = field(default_factory=lambda: MockDatagouvObject._mock_name())
