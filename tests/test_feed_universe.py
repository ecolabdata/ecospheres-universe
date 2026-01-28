from ecospheres_universe.feed_universe import UniverseOrg


class TestUniverseOrg:
    def test_as_json(self):
        org = UniverseOrg(id="foo", slug="bar", name="baz", type="qux")
        assert org.as_json() == {"id": "foo", "slug": "bar", "name": "baz", "type": "qux"}
