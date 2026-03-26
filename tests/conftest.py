import pytest


@pytest.fixture()
def mock_odbc_drivers(mocker):
    """Patches pyodbc.drivers() so tests run without a real ODBC driver installed."""
    return mocker.patch(
        "DataEngine.mssql.sqlconnectionobject.drivers",
        return_value=[
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
        ],
    )
