import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
import urllib
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import pyodbc


import pandas as pd

class dbOperation():
    """
        This class shall be used to hadle all db operations

        Written By: Max_AI
        Version : 1.0
        Revisions : None
    """
    def __init__(self, file_object, logger_object):
        self.filepath = 'RawDataFile/Input.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def dataBaseConnection(self, DatabaseName):
        """
                Method Name: dataBaseConnection
                Description: This method creates the database with the given name and if Database_operations already exists then opens the connection to the DB.
                Output: Connection to the DB
                On Failure: Raise ConnectionError

                 Written By: Max_AI
                Version: 1.0
                Revisions: None

                """
        try:
            conn = pyodbc.connect("DRIVER={SQL Server};"
                                  "SERVER=MIIPL_ON_KULDEE\SQLEXPRESS;"
                                  "DATABASE=TRACE_AI;"
                                  "UID=sa;"
                                  "PWD=P@ss1234;")
            self.logger_object.log(self.file_object, 'Opened %s database successfully' % DatabaseName)
            return conn

        except ConnectionError:
            self.logger_object.log(self.file_object, "Error while connecting to database: %s" % ConnectionError)
            raise ConnectionError

    def createTableDb(self, DatabaseName, TableName, column_dtype):
        """
                Method Name : createTableDb
                Description : This method creates a table in the given database which will be used to insert the Good data after raw data processing
                Output : None

                Written By : Max_AI
                Version : 1.0
                Revisions : None
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS ({TableName})'.format(TableName=TableName))

            for key in column_dtype.keys():
                type = column_dtype[key]
                try:
                    conn.execute('ALTER TABLE {TableName} ({column_name} {dataType})'.format(TableName = TableName, column_name=key, dataType=type))
                except:
                    conn.execute('CREATE TABLE {TableName} ({column_name} {dataType})'.format(TableName= TableName,column_name=key, dataType=type))
            conn.close()

        except Exception as e:
            raise e

    def insertdftoSQL(self, df, tablename):
        """
                Mehtod: insertTableGoodData
                Description: This method inserts the Good data files from the Good_Raw folder into the above created table
                Output: None
                On Failure: Raise Exception

            :param Database: Max_Cash_Out
                :return: None
        """
        self.logger_object.log(self.file_object, 'insertdftoSQL method of dbOperation class started')
        try:
            quoted = urllib.parse.quote_plus(
                "DRIVER={SQL Server};SERVER=MIIPL_ON_KULDEE\SQLEXPRESS;DATABASE=TRACE_AI;UID=sa;PWD=P@ss1234;")
            engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

            df.to_sql(tablename, schema='dbo', con=engine, chunksize=200, method='multi', index=False,
                             if_exists='replace')

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in insertdftoSQL of dboperation mehtod. Exception message: ', str(e))
            raise e



    def readTableIntodf(self, DatabaseName):
        """
                Method Name: Readtableintodf
                Description: This mehtod reads sql table as dataframe
                Output: Pandas Dataframe
                On Failure: Raise Exception
        """
        conn = self.dataBaseConnection(DatabaseName)
        try:
            df = pd.read_sql_query('select * from dbo.Trace_data;', conn)
            return df
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in readTableIntodf mehtod of dboperation class')
            raise e

   