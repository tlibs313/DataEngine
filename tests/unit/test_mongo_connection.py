import pytest

from DataEngine.mongo.mongoconnectionobject import MongoConnectionObject


class TestMongoConnectionObject:
    def _make(self, **overrides):
        kwargs = dict(name="mongo", server="mongoserver:27017", database="mydb", UN="user", PW="pass")
        kwargs.update(overrides)
        return MongoConnectionObject(**kwargs)

    def test_connection_string_format(self):
        obj = self._make()
        assert obj.connectionString == "mongodb://user:pass@mongoserver:27017/mydb?authSource=admin"

    def test_connection_string_scheme(self):
        obj = self._make()
        assert obj.connectionString.startswith("mongodb://")

    def test_connection_string_auth_source(self):
        obj = self._make()
        assert "authSource=admin" in obj.connectionString

    def test_name_is_stored(self):
        obj = self._make(name="reporting")
        assert obj.name == "reporting"

    def test_database_name_is_stored(self):
        obj = self._make(database="warehouse")
        assert obj.databaseName == "warehouse"

    def test_repr_contains_name_and_connection_string(self):
        obj = self._make()
        r = repr(obj)
        assert "MongoConnectionObject:mongo" in r
        assert "mongodb://user:pass@mongoserver:27017/mydb" in r
