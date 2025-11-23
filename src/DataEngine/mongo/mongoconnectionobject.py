import subprocess
from pymongo import MongoClient
from bson.codec_options import CodecOptions


class MongoResult:
    def __init__(self, isError, Result):
        self.IsError = isError
        self.Result = Result
        self.SuccessCount = 0
        self.ErrorCount = 0
        self.Total = int(self.SuccessCount) + int(self.ErrorCount)

    def __repr__(self):
        return f"MongoResult(**Result:{self.Result}, IsError:{self.IsError}, SuccessCount:{self.SuccessCount})"


def StdResponse(Message) -> MongoResult:
    ### Process Mongoimport.exe respose to get import metadata returns MongoResult ###
    irObj = MongoResult(False, "")
    isError = False
    r = Message.decode("utf-8")
    r_array = r.split("\n")
    l = len(r_array)
    if l < 1:
        # ImportResult(isError, "None")
        return irObj
    Result = r_array[0] if l < 3 else r_array[l - 2]
    Result = Result.split("\t")[1]
    for line in r_array:
        _detail = line.split("\t")
        i = len(_detail)
        if i > 0:
            s = _detail[i - 1].lower()
            if s.startswith("failed"):
                Result = s
                isError = True
                break
    irObj.IsError = isError
    irObj.Result = Result
    if isError == False:
        st = Result.split(". ")
        try:
            irObj.SuccessCount = int(st[0].split(" ")[0])
            irObj.ErrorCount = int(st[1].split(" ")[0])
            irObj.Total = irObj.SuccessCount + irObj.ErrorCount
        except:
            s = "response must not contain numbers"
    return irObj


class MongoConnectionObject:
    def __init__(self, **kwargs):
        # _template = "mssql+pyodbc://%s%s/%s?trusted_connection=%s&driver=SQL+Server+Native+Client+11.0"
        # "mongodb://%s:%s@%s/%s?authSource=admin"
        _template = "mongodb://%s:%s@%s/%s?authSource=admin"
        # _unpw = "" if _trusted == "yes" else urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw) + "@"
        # _unpw = urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw)
        name = kwargs["name"]
        server = kwargs["server"]
        database = kwargs["database"]
        _un = kwargs["UN"] if "UN" in kwargs.keys() else ""
        _pw = kwargs["PW"] if "PW" in kwargs.keys() else ""

        self.connectionString = _template % (_un, _pw, server, database)
        self.name = name
        # "mongodb://UN:PW@SVR/DB?authSource=admin"
        self.programFolder = "./exe"
        self.databaseName = database

    def __repr__(self):
        # return f"ConX = {self.connectionString}"
        return f"MongoConnectionObject:{self.name} = {self.connectionString}"

    def dropDatabase(self, database):
        cssnpiMongo = MongoClient(self.connectionString)
        # drop mongodb
        cssnpiMongo.drop_database(database)
        return f"{database} dropped from mongo."

    def query(self, collection, query):
        _CODEC_OPTIONS = CodecOptions(
            document_class=dict, unicode_decode_error_handler="replace"
        )
        db = MongoClient(self.connectionString)[self.databaseName]
        mongo_cursor = db.get_collection(collection, codec_options=_CODEC_OPTIONS).find(
            query
        )
        return mongo_cursor

    def aggregate(self, collection, query):
        _CODEC_OPTIONS = CodecOptions(
            document_class=dict, unicode_decode_error_handler="replace"
        )
        db = MongoClient(self.connectionString)[self.databaseName]
        mongo_cursor = db.get_collection(
            collection, codec_options=_CODEC_OPTIONS
        ).aggregate(query, allowDiskUse=True)
        return mongo_cursor

    def mongoImport(self, **kwargs) -> MongoResult:
        # mi = MongoInterface()
        mongo_command_txt_template = '{0}/mongoimport.exe --uri={1} --collection={2} --drop --file="{3}"{4} --numInsertionWorkers=10"'
        mongo_command_txt = mongo_command_txt_template.format(
            self.programFolder,
            self.connectionString,
            kwargs["collection"],
            kwargs["FullName"],
            " --jsonArray",
        )
        process = subprocess.Popen(
            mongo_command_txt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        # print(mongo_command)
        stdout, stderr = process.communicate()
        result = StdResponse(stdout)
        if result.IsError == True:
            ##try again but not as a JSON ARRAY, \O/
            mongo_command_txt = mongo_command_txt_template.format(
                self.programFolder,
                self.connectionString,
                kwargs["collection"],
                kwargs["FullName"],
                "",
            )
            process = subprocess.Popen(
                mongo_command_txt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            # print(mongo_command)
            stdout2, stderr2 = process.communicate()
            result.Error = "UrlError.FORMATTING"
            result = StdResponse(stdout2)
        return result
