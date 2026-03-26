import pytest
import urllib.parse

from DataEngine.mssql.sqlconnectionobject import (
    _get_pyodbc_driver,
    _build_odbc_string,
    SqlConnectionObject,
)

_DRIVER_18 = "ODBC Driver 18 for SQL Server"
_DRIVER_17 = "ODBC Driver 17 for SQL Server"
_ALL_DRIVERS = [_DRIVER_18, _DRIVER_17, "SQL Server Native Client 11.0", "SQL Server"]


# ---------------------------------------------------------------------------
# _get_pyodbc_driver
# ---------------------------------------------------------------------------

class TestGetPyodbcDriver:
    def test_prefers_driver_18_when_all_present(self, mocker):
        mocker.patch("DataEngine.mssql.sqlconnectionobject.drivers", return_value=_ALL_DRIVERS)
        assert _get_pyodbc_driver() == _DRIVER_18

    def test_falls_back_to_driver_17(self, mocker):
        mocker.patch(
            "DataEngine.mssql.sqlconnectionobject.drivers",
            return_value=[_DRIVER_17, "SQL Server"],
        )
        assert _get_pyodbc_driver() == _DRIVER_17

    def test_raises_when_no_supported_driver(self, mocker):
        mocker.patch("DataEngine.mssql.sqlconnectionobject.drivers", return_value=[])
        with pytest.raises(RuntimeError):
            _get_pyodbc_driver()


# ---------------------------------------------------------------------------
# _build_odbc_string — one test per auth mode
# ---------------------------------------------------------------------------

class TestBuildOdbcString:
    def test_windows_auth(self, mock_odbc_drivers):
        result = _build_odbc_string("srv", "db", trusted=True)
        assert "Trusted_Connection=yes;" in result
        assert "UID=" not in result
        assert "PWD=" not in result
        assert "Authentication=" not in result

    def test_sql_server_auth(self, mock_odbc_drivers):
        result = _build_odbc_string("srv", "db", UN="user", PW="pass")
        assert "UID=user;" in result
        assert "PWD=pass;" in result
        assert "Authentication=" not in result
        assert "Trusted_Connection=" not in result

    def test_aad_interactive(self, mock_odbc_drivers):
        result = _build_odbc_string("srv", "db", UN="user@domain.com")
        assert "Authentication=ActiveDirectoryInteractive;" in result
        assert "UID=user@domain.com;" in result
        assert "PWD=" not in result

    def test_aad_integrated(self, mock_odbc_drivers):
        result = _build_odbc_string("srv", "db")
        assert "Authentication=ActiveDirectoryIntegrated;" in result
        assert "UID=" not in result
        assert "PWD=" not in result

    def test_driver_is_included(self, mock_odbc_drivers):
        result = _build_odbc_string("srv", "db", trusted=True)
        assert f"DRIVER={{{_DRIVER_18}}}" in result

    def test_server_and_database_are_included(self, mock_odbc_drivers):
        result = _build_odbc_string("srv01", "db01", trusted=True)
        assert "SERVER=srv01;" in result
        assert "DATABASE=db01;" in result

    def test_trust_server_certificate_always_present(self, mock_odbc_drivers):
        for kwargs in [
            {"trusted": True},
            {"UN": "u", "PW": "p"},
            {"UN": "u"},
            {},
        ]:
            assert "TrustServerCertificate=yes;" in _build_odbc_string("srv", "db", **kwargs)


# ---------------------------------------------------------------------------
# SqlConnectionObject — trusted string normalisation (the .env contract)
# ---------------------------------------------------------------------------

class TestSqlConnectionObjectInit:
    def _make(self, mocker, **kwargs):
        mocker.patch("DataEngine.mssql.sqlconnectionobject.drivers", return_value=[_DRIVER_18])
        mocker.patch("DataEngine.mssql.sqlconnectionobject.create_engine")
        mocker.patch("DataEngine.mssql.sqlconnectionobject.scoped_session")
        mocker.patch("DataEngine.mssql.sqlconnectionobject.sessionmaker")
        return SqlConnectionObject(name="test", server="srv", database="db", **kwargs)

    def _decoded(self, obj):
        return urllib.parse.unquote_plus(obj.connection)

    def test_trusted_yes_string_uses_windows_auth(self, mocker):
        obj = self._make(mocker, trusted="yes")
        assert "Trusted_Connection=yes" in self._decoded(obj)

    def test_trusted_no_string_with_credentials_uses_sql_auth(self, mocker):
        # Regression: "no" is a truthy string — must NOT be treated as Windows auth
        obj = self._make(mocker, trusted="no", UN="user", PW="pass")
        decoded = self._decoded(obj)
        assert "UID=user" in decoded
        assert "PWD=pass" in decoded
        assert "Trusted_Connection=yes" not in decoded

    def test_trusted_bool_true_uses_windows_auth(self, mocker):
        obj = self._make(mocker, trusted=True)
        assert "Trusted_Connection=yes" in self._decoded(obj)

    def test_trusted_bool_false_with_no_credentials_uses_aad_integrated(self, mocker):
        obj = self._make(mocker, trusted=False)
        assert "ActiveDirectoryIntegrated" in self._decoded(obj)

    def test_trusted_absent_with_un_only_uses_aad_interactive(self, mocker):
        obj = self._make(mocker, UN="user@domain.com")
        assert "ActiveDirectoryInteractive" in self._decoded(obj)

    def test_repr(self, mocker):
        mock_engine = mocker.MagicMock()
        mock_engine.url.host = "srv"
        mock_engine.url.database = "db"
        mocker.patch("DataEngine.mssql.sqlconnectionobject.drivers", return_value=[_DRIVER_18])
        mocker.patch("DataEngine.mssql.sqlconnectionobject.create_engine", return_value=mock_engine)
        mocker.patch("DataEngine.mssql.sqlconnectionobject.scoped_session")
        mocker.patch("DataEngine.mssql.sqlconnectionobject.sessionmaker")
        obj = SqlConnectionObject(name="myconn", server="srv", database="db", trusted="yes")
        assert repr(obj) == "SqlConnectionObject:myconn = srv.db"
