from . import _version
import os
import importlib
import pathlib
import warnings
import sys
import json
from dotenv import load_dotenv

from .mssql import SqlConnectionObject
from .postgres import PgConnectionObject
from .mongo import MongoConnectionObject

__version__ = _version.get_version()
alchemyConnections = {}
alchemyObjects = {}


def help():
    print("Interface for creating wrappers for Database Objects")
    print("PgConnectionObject() for postres")
    print("SqlConnectionObject() for SQL server")
    print("MongoConnectionObject() for Mongo")
    print(
        "format for a database connection document which are strings saved in 'database.env':"
    )
    print(
        '{"$name":{"type":"[mssql|postgres|mongo]","database":"","server":"","UN":"","PW":"","trusted":"[yes|no]"}}'
    )
    print(
        "where $name is the contextual name for the connextion, like 'main','data', or 'library'"
    )
    print(
        "execute connectionStringMaker() to start the connection string builder"
    )
    print("Active Connection Object Names:")
    print("\r\n".join(alchemyObjects.keys()))
    print("-------")
    print("active connections strings:")
    print(f"alchemyConnections = {alchemyConnections}")

    print("-------")
    print("active connections objects")
    print(f"alchemyObjects = {alchemyObjects}")


def connectionGenerator(connectionList: dict = None):
    global alchemyObjects
    global alchemyConnections
    if connectionList is None:
        load_dotenv("database.env")
        alchemyConnections = json.loads(os.environ["databases"])
        connectionList = alchemyConnections
    for dbs in connectionList.keys():
        server = connectionList[dbs]["server"]
        database = connectionList[dbs]["database"]
        un = connectionList[dbs]["UN"]
        pw = connectionList[dbs]["PW"]
        trusted = connectionList[dbs]["trusted"]
        _t = connectionList[dbs]["type"]
        if _t == "mssql":
            alchemyObjects[dbs] = SqlConnectionObject(
                **{
                    "name": dbs,
                    "server": server,
                    "database": database,
                    "UN": un,
                    "PW": pw,
                    "trusted": trusted,
                }
            )
        if _t == "postgres":
            alchemyObjects[dbs] = PgConnectionObject(
                **{
                    "name": dbs,
                    "server": server,
                    "database": database,
                    "UN": un,
                    "PW": pw,
                }
            )
        if _t == "mongo":
            alchemyObjects[dbs] = MongoConnectionObject(
                **{
                    "name": dbs,
                    "server": server,
                    "database": database,
                    "UN": un,
                    "PW": pw,
                }
            )


def saveConnectionStrings():
    cx = str(alchemyConnections).replace("'", '"')
    s = f"databases = '{cx}'"
    with open("./database.env", "w") as f:
        f.write(s)


def connectionStringMaker():
    def validate(question, valid_responses):
        response = ""
        while True:
            _res = input(question)
            if _res in valid_responses:
                response = _res
                break
            else:
                print(f"valid reponses are: {','.join(valid_responses)}")
        return response

    def notBlank(question):
        response = ""
        while True:
            _res = input(question)
            if len(_res) > 0:
                response = _res
                break
            else:
                print(f"response must not be blank")
        return response

    _svr_type = validate("1=MSSQL | 2=POSTGRES | 3=MONGO?\r\n", ["1", "2", "3"])
    svr_type = ""
    if _svr_type == "1":
        svr_type = "mssql"
    if _svr_type == "2":
        svr_type = "postgres"
    if _svr_type == "3":
        svr_type = "mongo"

    # svr_type = "mssql" if _svr_type == "1"  else "postgres"
    server = notBlank("Server name?\r\n")
    database = notBlank("database name?\r\n")
    trusted = "no"
    username = ""
    password = ""
    if svr_type == "mssql":
        _auth = validate("Windows Authentication? (Y/n)\r\n", ["Y", "n"])
        trusted = "yes" if _auth == "Y" else "no"
    if trusted == "no":
        username = notBlank("User name?\r\n")
        password = notBlank("Password?\r\n")
    name = notBlank("Finally, give this connection a name?")
    global alchemyConnections
    __conx = {
        "type": svr_type,
        "database": database,
        "server": server,
        "UN": username,
        "PW": password,
        "trusted": trusted,
    }
    print(__conx)
    correct = validate("is this correct? (Y/n)\r\n", ["Y", "n"])
    if correct == "n":
        connectionStringMaker()
        return
    alchemyConnections[name] = __conx
    more = validate("Add another?(Y/n)", ["Y", "n"])
    if more == "Y":
        connectionStringMaker()
    saveConnectionStrings()


_import_dbdotenv = load_dotenv("database.env")
if _import_dbdotenv:
    alchemyConnections = json.loads(os.environ["databases"])
    connectionGenerator()
else:
    with open("database.env", "w+") as dbf:
        dbf.write("databases = '{}'")
    print(
        "the database.env file is empty. Starting the Connection String Maker function:"
    )
    connectionStringMaker()

