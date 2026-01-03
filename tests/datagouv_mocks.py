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

    @property
    def element_class(self) -> ElementClass:
        # FIXME: this can fail
        return ElementClass[self.__class__.__name__]

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
        }

    @classmethod
    def one(cls) -> Self:
        return cls()

    @classmethod
    def some(cls, n: int) -> list[Self]:
        return [cls() for _ in range(n)]


@final
class Organization(DatagouvObject):
    _objects: list[DatagouvObject]

    def __init__(self, objects: list[DatagouvObject] | None = None):
        super().__init__()
        self._objects = objects if objects else []

    def __repr__(self) -> str:
        return f"<{self.id} {[o.id for o in self._objects]}>"

    def objects(self, element_class: ElementClass | None = None) -> list[DatagouvObject]:
        return (
            [o for o in self._objects if o.element_class is element_class]
            if element_class
            else self._objects
        )

    def add(self, *elements: DatagouvObject):
        self._objects += elements


# FIXME: would be better as a mixin
# FIXME: better x_owned/owned_by
class OwnedDatagouvObject(DatagouvObject):
    _organization: Organization

    def __repr__(self) -> str:
        return f"<{self.id} @{self._organization.id}>"

    @property
    def organization(self) -> Organization:
        return self._organization

    @override
    def as_dict(self) -> dict[str, Any]:
        return {
            **super().as_dict(),
            "organization": self.organization.as_dict(),
        }

    def owned_by(self, organization: Organization) -> Self:
        self._organization = organization
        organization.add(self)
        return self

    @classmethod
    def one_owned(cls, organization: Organization) -> Self:
        return cls().owned_by(organization)

    @classmethod
    def some_owned(cls, n: int, organizations: list[Organization]) -> list[Self]:
        return [cls().owned_by(org) for org in islice(cycle(organizations), n)]


@final
class Dataset(OwnedDatagouvObject):
    pass


@final
class Dataservice(OwnedDatagouvObject):
    pass


@final
class Element(DatagouvObject):
    _object: DatagouvObject

    # FIXME: restrict to "acceptable" DatagouvObject
    def __init__(self, object: DatagouvObject):
        super().__init__()
        self._object = object

    @property
    def object(self):
        return self._object


@final
class Topic(OwnedDatagouvObject):
    _elements: list[Element]

    def __init__(self, *elements: DatagouvObject):
        super().__init__()
        self._elements = [Element(e) for e in elements] if elements else []

    def __repr__(self) -> str:
        return f"<{self.id} {[e.object for e in self._elements]}>"

    def elements(
        self, element_class: ElementClass | None = None, organization: Organization | None = None
    ) -> list[Element]:
        elements = self._elements
        if element_class:
            elements = filter(lambda e: e.object.element_class is element_class, elements)
        if organization:
            elements = filter(lambda e: e.object.organization == organization, elements)
        return list(elements)

    def organizations(self, element_class: ElementClass | None = None) -> list[Organization]:
        # FIXME: typing
        return {e.object.organization for e in self.elements(element_class)}

    def clone(self) -> Self:
        clone = copy(self)
        clone._elements = [e for e in self._elements]
        return clone

    def add(self, *elements: DatagouvObject) -> Self:
        self._elements += [Element(e) for e in elements]
        return self

    def remove(self, *elements: DatagouvObject) -> Self:
        self._elements = [e for e in self._elements if e.object not in elements]
        return self
