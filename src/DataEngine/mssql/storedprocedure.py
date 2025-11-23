from __future__ import annotations

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
    TextClause
)

class Parameter:
    def __init__(self, sql_parameter, parameter =None, output=False):
        self.sql_parameter = sql_parameter
        self.parameter = parameter
        self.output = output
    def __str__(self):
        _frmt = '' if self.parameter is None else " = " + self.parameter
        _frmt = _frmt + '' if self.output==False else ' output'
        return f"{self.sql_parameter}{_frmt}"

class ParameterCollection:
    def __init__(self, parameters:list(Parameter)):
        self.parameters = parameters
    def add(self, parameter:Parameter)->int:
        """
        Add a predefined mapped column to the current list.
        """
        if self.parameters is None:
            self.parameters = []
        self.parameters.append(parameter)
        return self.parameters.index(parameter)
    def find(self, name:str)->Parameter:
        r = [f for f in self.parameters if f.sql_parameter == name]
        if r:
            return r[0]
        r = [f for f in self.parameters if f.parameter == name]
        if r:
            return r[0]
    def findAt(self, index:int)-> Parameter:
        return self.parameters[index]


class StoredProcedure:
    def __init__(self, name, procedure, parameters:dict):
        """
        name: A friendly name to call this procedure.

        procedure: the schema qualified name of the procedure, as is in the database

        params: a dictionary of paramters for this procedure.
        e.g. {'p1':'some value', 'p2':'some other value'}
        if params are set, make sure they map correctly to the procedures params:
            'dbo.who2 @loginname = :name',{'name':'domainname'}
        You can add parameters by manipulating a standard dict object:
        StoredProcedure.parameters["param_name"] = "param value".
        Just make sure the stored procedure definition has the cooresponding sql parameter (@param_name = :param_name)

        at the time of execution, the procedure and the params will be seperate:
        con.execute(this.command, this.parameters), so this.command needs to be a TextClause object)
        the __str__ will show you what the full procedure call will look like once SQL gets it... 
        """
        self.name = name
        self.procedure = procedure
        _sql = f"""
            SET NOCOUNT ON;
            EXEC {self.procedure};
        """
        self.command = text(_sql)
        self.parameters = parameters
    def __str__(self):
         return str(self.procedure.bindparams(**self.parameters))
    def compile(self,**kwargs ):
        return self.procedure.bindparams(**self.parameters).compile(**kwargs)
        