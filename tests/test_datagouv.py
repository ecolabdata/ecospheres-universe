import pytest

from ecospheres_universe.datagouv import (
    DatagouvObject,
    Dataservice,
    Dataset,
    Organization,
    Topic,
)


DATAGOUV_OBJECTS = (
    Dataservice,
    Dataset,
    Organization,
    Topic,
)

TOPIC_OBJECTS = (
    Dataservice,
    Dataset,
)


class TestDatagouvObject:
    def test_object_name(self):
        for clazz in DATAGOUV_OBJECTS:
            assert clazz.object_name() == clazz.__name__

    def test_namespace(self):
        for clazz in DATAGOUV_OBJECTS:
            assert clazz.namespace() == clazz.object_name().lower() + "s"

    def test_class_from_name(self):
        for clazz in DATAGOUV_OBJECTS:
            assert DatagouvObject.class_from_name(clazz.object_name()) == clazz
            assert DatagouvObject.class_from_name(clazz.object_name().lower()) == clazz
            assert DatagouvObject.class_from_name(clazz.object_name().upper()) == clazz

        # class doesn't exist
        with pytest.raises(TypeError):
            DatagouvObject.class_from_name("Foo")

        # class exists but isn't a DatagouvObject
        with pytest.raises(TypeError):
            DatagouvObject.class_from_name("DatagouvApi")


class TestOrganization:
    def test_ordering(self):
        assert Organization("foo", "foo", "foo") == Organization("foo", "foo", "foo")
        assert Organization("foo", "foo", "foo") != Organization("bar", "foo", "foo")
        assert Organization("foo", "foo", "foo") != Organization("foo", "bar", "foo")
        assert Organization("foo", "foo", "foo") != Organization("foo", "foo", "bar")

        assert Organization("bar", "bar", "bar") < Organization("foo", "foo", "foo")
        assert Organization("foo", "foo", "foo") < Organization("foo", "foo 1", "foo-1")
        assert Organization("foo", "foo", "foo") < Organization("foo", "foo", "foo-1")
