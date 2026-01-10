from copy import copy
from itertools import cycle, islice
from typing import final, override, Any, Self

from ecospheres_universe.datagouv import ElementClass


class DatagouvObject:
    _id_counter: int = 0

    def __init__(self):
        DatagouvObject._id_counter += 1
        self._id: int = DatagouvObject._id_counter

    def __repr__(self) -> str:
        return f"<{self.id}>"

    @property
    def id(self) -> str:
        return f"{self.__class__.__name__.lower()}-{self._id}"

    @property
    def slug(self) -> str:
        return f"{self.__class__.__name__.lower()}-slug-{self._id}"

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__} {self._id}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
        }


@final
class Organization(DatagouvObject):
    _TYPES: list[str | None] = ["type-A", None, "type-B", "type-C"]

    _objects: list["DatagouvRecord"]

    def __init__(self, objects: list["DatagouvRecord"] | None = None):
        super().__init__()
        self._objects = objects if objects else []
        self._type = Organization._TYPES[self._id % len(Organization._TYPES)]

    def __repr__(self) -> str:
        return f"<{self.id} {[o.id for o in self._objects]}>"

    @property
    def type(self) -> str | None:
        return self._type

    def objects(self, element_class: ElementClass | None = None) -> list["DatagouvRecord"]:
        return (
            [o for o in self._objects if o.element_class is element_class]
            if element_class
            else self._objects
        )

    @override
    def as_dict(self) -> dict[str, Any]:
        return {
            **super().as_dict(),
            "type": self._type,
        }

    def with_type(self, type: str | None) -> Self:
        self._type = type
        return self

    def add_elements(self, *elements: "DatagouvRecord") -> Self:
        self._objects += elements
        return self

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def some(cls, n: int) -> list[Self]:
        return [cls() for _ in range(n)]


class DatagouvRecord(DatagouvObject):
    _organization: Organization

    def __repr__(self) -> str:
        return f"<{self.id} @{self._organization.id}>"

    @property
    def organization(self) -> Organization:
        return self._organization

    @property
    def element_class(self) -> ElementClass:
        # TODO: this can fail, should only exist when valid
        return ElementClass[self.__class__.__name__]

    @override
    def as_dict(self) -> dict[str, Any]:
        return {
            **super().as_dict(),
            "organization": self.organization.as_dict(),
        }

    def with_owner(self, organization: Organization) -> Self:
        self._organization = organization
        organization.add_elements(self)
        return self

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def some(cls, n: int, organizations: list[Organization]) -> list[Self]:
        return [cls().with_owner(org) for org in islice(cycle(organizations), n)]


@final
class Dataset(DatagouvRecord):
    pass


@final
class Dataservice(DatagouvRecord):
    pass


@final
class Element(DatagouvObject):
    _object: DatagouvRecord

    def __init__(self, object: DatagouvRecord):
        super().__init__()
        self._object = object

    def __repr__(self) -> str:
        return f"<{self.id} [{self._object.id}]>"

    @property
    def object(self) -> DatagouvRecord:
        return self._object


@final
class Topic(DatagouvRecord):
    _elements: list[Element]

    def __init__(self, *elements: DatagouvRecord):
        super().__init__()
        self._elements = [Element(e) for e in elements] if elements else []

    def __repr__(self) -> str:
        return f"<{self.id} {self._elements}>"

    def elements(self, element_class: ElementClass | None = None) -> list[Element]:
        elements = self._elements
        if element_class:
            elements = filter(lambda e: e.object.element_class is element_class, elements)
        return list(elements)

    def organizations(self, element_class: ElementClass | None = None) -> list[Organization]:
        return list({e.object.organization for e in self.elements(element_class)})

    def clone(self) -> Self:
        clone = copy(self)
        clone._elements = [e for e in self._elements]
        return clone

    def add_elements(self, *elements: DatagouvRecord) -> Self:
        self._elements += [Element(e) for e in elements]
        return self

    def remove_elements(self, *elements: DatagouvRecord) -> Self:
        self._elements = [e for e in self._elements if e.object not in elements]
        return self
