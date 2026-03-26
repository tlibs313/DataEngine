import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
from pyodbc import drivers
from .storedprocedure import StoredProcedure


def _get_pyodbc_driver() -> str:
    prefs = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    installed = set(drivers())
    for drv in prefs:
        if drv in installed:
            return drv
    raise RuntimeError("No supported SQL Server ODBC driver found.")


def _build_odbc_string(server: str, database: str, UN: str | None = None, PW: str | None = None, trusted: bool = False) -> str:
    """
    Builds a pyodbc connection string for one of three SQL Server auth modes:

      1. Windows Authentication  (trusted=True)
         Uses the current Windows identity — no credentials required.

      2. SQL Server Authentication  (UN + PW provided)
         Classic username/password login against SQL Server's own user store.

      3. Azure Active Directory
         a. ActiveDirectoryIntegrated  (no UN, no PW) — silent SSO using a
            cached token from a prior interactive session. Zero prompts when
            a valid token exists.
         b. ActiveDirectoryInteractive  (UN provided, no PW) — MFA browser
            pop-up, pre-filled with the supplied username. The ODBC driver
            caches the resulting token so subsequent calls within ~1 hour
            fall through to (a).
    """
    base = (
        f"DRIVER={{{_get_pyodbc_driver()}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"TrustServerCertificate=yes;"
    )

    if trusted:
        return base + "Trusted_Connection=yes;"

    if UN is not None and PW is not None:
        return base + f"UID={UN};PWD={PW};"

    if UN is not None:
        return base + f"Authentication=ActiveDirectoryInteractive;UID={UN};"

    return base + "Authentication=ActiveDirectoryIntegrated;"


class SqlConnectionObject:
    def __init__(self, **kwargs):
        name     = kwargs["name"]
        server   = kwargs["server"]
        database = kwargs["database"]
        un       = kwargs.get("UN")
        pw       = kwargs.get("PW")
        trusted  = kwargs.get("trusted", False)
        if isinstance(trusted, str):
            trusted = trusted.strip().lower() == "yes"

        odbc = _build_odbc_string(server, database, UN=un, PW=pw, trusted=trusted)
        conx = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc)}"

        self.name       = name
        self.connection = conx
        self.engine     = create_engine(conx, fast_executemany=True)
        self.session    = scoped_session(sessionmaker(bind=self.engine))

    def __repr__(self):
        return f"SqlConnectionObject:{self.name} = {self.engine.url.host}.{self.engine.url.database}"

    def getConnection(self):
        return self.engine.connect()

    def getStreamingConnection(self):
        return self.engine.connect().execution_options(stream_results=True)

    def executeProcedure(self, procedure_name, streaming=False):
        con = self.getConnection if streaming == False else self.getStreamingConnection
        with con() as tconn:
            try:
                tconn.execute(text(f"set nocount on; exec {procedure_name};"))
                tconn.commit()
            except Exception as tctE:
                tconn.rollback()
                raise Exception(f"executeProcedure(): {tctE}")

    def truncateTable(self, schema, name):
        with self.getConnection() as tconn:
            try:
                tconn.execute(text(f"truncate table {schema}.{name};"))
                tconn.commit()
            except Exception as tctE:
                tconn.rollback()
                raise Exception(f"executeProcedure(): {tctE}")
                return

    def interop(self, insert_command) -> int:
        # insert_command = insert_command + "; select scope_identity() as new_id;"
        # id = -1
        con = self.engine.raw_connection()
        cur = con.cursor()
        cur.execute(insert_command)
        newid = cur.execute("select scope_identity() as new_id").fetchone()
        # cur.nextset()
        # id = cur.fetchone()
        con.commit()
        return int(newid[0])

    def query(self, query_text):
        statement = text(query_text)
        with self.getConnection() as con:
            results = con.execute(statement).fetchall()
            con.commit()
        return results

    def execute(self, non_result_query):
        statement = text(non_result_query)
        with self.getConnection() as con:
            con.execute(statement)
            con.commit()
        # return results

    def queryStream(self, query_text):
        statement = text(query_text)
        with self.getStreamingConnection() as con:
            for item in con.execute(statement).fetchall():
                yield item
            con.commit()

    def getTable(self, query_text) -> pd.DataFrame:
        statement = text(query_text)
        with self.getConnection() as con:
            df = pd.read_sql(statement, con)
        return df

    def chunkTable(self, query_text, chunksize):
        statement = text(query_text)
        with self.getStreamingConnection() as con:
            for chunk in pd.read_sql(statement, con, chunksize=chunksize):
                yield chunk
