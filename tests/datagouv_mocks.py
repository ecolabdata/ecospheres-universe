from typing import final, Protocol

from ecospheres_universe.datagouv import ElementClass


class DatagouvObject:
    _id_counter: int = 0

    def __init__(self):
        DatagouvObject._id_counter += 1
        self._id: int = DatagouvObject._id_counter
        print(f"+ {self.id}")

    @property
    def id(self) -> str:
        return f"{self.__class__.__name__.lower()}-{self._id}"

    @property
    def slug(self) -> str:
        return f"{self.__class__.__name__.lower()}-slug-{self._id}"

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__} {self._id}"

    @classmethod
    def one[T](cls: type[T]) -> T:
        return cls()

    @classmethod
    def some[T](cls: type[T], n: int) -> list[T]:
        return [cls() for _ in range(n)]


class ElementObject(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def element_class(self) -> ElementClass: ...


class ElementMixin:
    @property
    def element_class(self) -> ElementClass:
        return ElementClass[self.__class__.__name__]


@final
class Dataset(ElementMixin, DatagouvObject):
    pass


@final
class Dataservice(ElementMixin, DatagouvObject):
    pass


@final
class Organization(DatagouvObject):
    _objects: dict[ElementClass, list[DatagouvObject]]

    def __init__(self, objects: dict[ElementClass, list[DatagouvObject]] | None = None):
        super().__init__()
        self._objects = objects if objects else {}

    def objects(self, element_class: ElementClass) -> list[DatagouvObject]:
        return self._objects.get(element_class, []) if self._objects else []


@final
class Element(DatagouvObject):
    _object: ElementObject

    def __init__(self, object: ElementObject):
        super().__init__()
        self._object = object

    @property
    def element_id(self) -> str:
        return self._object.id

    @property
    def element_class(self) -> ElementClass:
        return self._object.element_class


@final
class Topic(DatagouvObject):
    _elements: list[Element]

    def __init__(self, elements: list[Element]):
        super().__init__()
        self._elements = elements

    def elements(self, element_class: ElementClass) -> list[Element]:
        return [e for e in self._elements if e.element_class is element_class]
