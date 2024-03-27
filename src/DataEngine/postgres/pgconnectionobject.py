import urllib.parse
from sqlalchemy import (
    create_engine,
    inspect,
    select,
    text,
    Column,
    Integer,
    DateTime,
    String,
    func,
    ForeignKey,
    Boolean,
    Index,
)
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd

class PgConnectionObject:
    def __init__(self, **kwargs):
        _template = "postgresql+psycopg2://%s@%s/%s"
        # {"UN": "", "PW": "", "trusted": "yes"}
        name = kwargs["name"]
        server = kwargs["server"]
        database = kwargs["database"]
        _un = kwargs["UN"] if "UN" in kwargs.keys() else ""
        _pw = kwargs["PW"] if "PW" in kwargs.keys() else ""
        # _trusted = kwargs["trusted"] if "trusted" in kwargs.keys() else ""
        # _unpw = "" if _trusted = "yes" else urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw)
        _unpw = urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw)
        conx = _template % (_unpw, server, database)
        self.name = name
        # self.connection = makeConnection(self.name)
        self.connection = conx
        self.engine = create_engine(
            conx,
            executemany_mode="values_plus_batch",
            insertmanyvalues_page_size=5000,
            executemany_batch_page_size=500,
        )
        # self.engine = makePgEngine(makePgConnection(self.name))
        self.session = scoped_session(sessionmaker(bind=self.engine))

    def __repr__(self):
        return f"PgConnectionObject:{self.name} = {self.engine.url.host}.{self.engine.url.database}"

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
                raise Exception(f"truncateTable(): {tctE}")
                return

    def interop_returnind_id(self, insert_command) -> int:
        # insert_command = insert_command + " returning ID"
        # id = -1
        con = self.engine.raw_connection()
        cur = con.cursor()
        cur.execute(insert_command)
        # newid = cur.execute("select scope_identity() as new_id").fetchone()
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
