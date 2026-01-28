"""
Mocks for datagouv objects.

All mock-only members are suffixed with _m, except for private and static members.

When applicable, narrow-typed mocked members are accessible via their m-suffix equivalent.
For instance:
- MockTopic().organization returns an Organization instance like Topic().organization.
- MockTopic().organization_m returns the underlying MockOrganization instance.

Mock classes hide most of their fields from __init__ to avoid interferring with automatic
initialization. Builder-like methods are provided when overriding fields is relevant.

Mock$Class must precede $Class when declaring inheritance, or it'll break automatic
initialization. So:
- class Organization(MockDatagouvObject, Organization) => OK
- class Organization(Organization, MockDatagouvObject) => KO
"""

import builtins

from abc import ABC
from copy import copy
from dataclasses import dataclass, field, InitVar
from itertools import cycle, islice
from typing import cast, override, ClassVar, Self, TypeAlias

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


@dataclass
class MockDatagouvObject(DatagouvObject, ABC):
    _ID_COUNTER: ClassVar[int] = 0

    # mocked fields
    id: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_id())
    slug: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_slug())

    # mock-only fields
    _id: int = field(init=False, default_factory=lambda: MockDatagouvObject._next_id())

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

    # mocked fields
    name: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_name())

    # mock-only fields
    type_m: str | None = field(  # shortcut avoids separate MockUniverseOrg
        init=False, default=None
    )
    _objects: list["MockOwnedObject"] = field(init=False)

    # init-only
    mock_objects: InitVar[list["MockOwnedObject"] | None] = None

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def many(cls, n: int) -> list[Self]:
        return [cls() for _ in range(n)]

    def __post_init__(self, mock_objects: list["MockOwnedObject"] | None = None):
        self.type_m = self._CATEGORIES[self._id % len(self._CATEGORIES)]
        self._objects = mock_objects if mock_objects else []

    def __hash__(self) -> int:
        return hash(self.id)

    @override
    def __repr__(self) -> str:
        return f"<{self.id} {[o.id for o in self._objects]}>"

    def objects_m(
        self, object_class: builtins.type[OwnedObject] | None = None
    ) -> list["MockOwnedObject"]:
        return (
            [o for o in self._objects if isinstance(o, object_class)]
            if object_class
            else self._objects
        )

    def with_type_m(self, type: str | None) -> Self:
        self.type_m = type
        print(self.as_json())
        return self

    def add_objects_m(self, *mock_objects: "MockOwnedObject") -> Self:
        self._objects += mock_objects
        return self


@dataclass
class MockOwnedObject(MockDatagouvObject, OwnedObject, ABC):
    # mocked fields
    organization: Organization | None = field(init=False, default=None)

    @classmethod
    def one(cls, organization: MockOrganization | None = None) -> Self:
        return cls().with_ownership_m(organization)

    @classmethod
    def many(cls, n: int, organizations: list[MockOrganization]) -> list[Self]:
        return [cls().with_ownership_m(org) for org in islice(cycle(organizations), n)]

    @override
    def __repr__(self) -> str:
        return f"<{self.id} @{self.organization.id if self.organization else 'noone'}>"

    @property
    def organization_m(self) -> MockOrganization | None:
        """Narrow-typed accessor to the corresponding original property."""
        # safe to cast since builders create MockOrganization
        return cast(MockOrganization, self.organization)

    def with_ownership_m(self, organization: MockOrganization | None) -> Self:
        self.organization = organization
        if organization:
            organization.add_objects_m(self)
        return self


@dataclass
class MockDataset(MockOwnedObject, Dataset):
    # mocked fields
    title: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_name())


@dataclass
class MockDataservice(MockOwnedObject, Dataservice):
    # mocked fields
    title: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_name())


MockTopicObject: TypeAlias = MockDataset | MockDataservice


@dataclass
class MockTopicElement(TopicElement):
    # mocked fields
    id: str = field(init=False)
    object_id: str = field(init=False)

    # mock-only fields
    object_m: MockTopicObject

    def __post_init__(self):
        self.id = f"element-{self.object_m.id}"
        self.object_id = self.object_m.id

    @override
    def __repr__(self) -> str:
        return f"<{self.id} [{self.object_m.id}]>"


@dataclass
class MockTopic(MockOwnedObject, Topic):
    # mocked fields
    name: str = field(init=False, default_factory=lambda: MockDatagouvObject._mock_name())

    # mock-only fields
    _elements: list[MockTopicElement] = field(init=False)

    # init-only
    mock_elements: InitVar[list[MockTopicObject]] = []  # default unused, @dataclass constraint

    def __post_init__(self, mock_elements: list[MockTopicObject]):
        self._elements = [MockTopicElement(e) for e in mock_elements] if mock_elements else []

    @override
    def __repr__(self) -> str:
        return f"<{self.id} {self._elements}>"

    def elements_m(self, object_class: type[TopicObject] | None = None) -> list[MockTopicElement]:
        elements = self._elements
        if object_class:
            elements = filter(lambda e: type(e.object_m) is object_class, elements)
        return list(elements)

    def organizations_m(
        self, object_class: type[TopicObject] | None = None
    ) -> list[MockOrganization]:
        return uniquify(
            org for e in self.elements_m(object_class) if (org := e.object_m.organization_m)
        )

    def add_elements_m(self, *mock_elements: MockTopicObject) -> Self:
        self._elements += [MockTopicElement(e) for e in mock_elements]
        return self

    def remove_elements_m(self, *mock_elements: MockTopicObject) -> Self:
        self._elements = [e for e in self._elements if e.object_m not in mock_elements]
        return self

    def clone(self) -> Self:
        clone = copy(self)
        clone._elements = [e for e in self._elements]
        return clone
