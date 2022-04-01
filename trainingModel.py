"""
This is the Entry point for Training the Machine Learning Model.

Written By: Max_AI
Version: 1.0
Revisions: None

"""
# Doing the necessary imports
import numpy as np
import pandas as pd
#from sqlalchemy import create_engine
import urllib
import pyodbc
from file_operations import file_methods
from application_logging import logger
import pickle
from Database_operations.dboperation import dbOperation
from DataPreparation.prophet_model import prophet
from Trace_Count_methods.tracemehtods import tracemethod
from Trace_Count_methods.category_wise_codes import category_codes

class trainModel:
    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
        self.dboperation = dbOperation(self.file_object, self.log_writer)
        self.prophet = prophet(self.file_object, self.log_writer)
        self.tracem = tracemethod(self.file_object, self.log_writer,self.prophet)
        self.categorycode = category_codes(self.file_object, self.log_writer, self.tracem)

    def trainingModel(self):
        # # Logging the start of training
        # self.log_writer.log(self.file_object, 'Start of Training')
        try:
            """Getting the data from the source"""
            #df = self.dboperation.readTableIntodf('TRACE_AI')

            #running overall counts method which save csv file containing counts
            df = self.dboperation.readTableIntodf('TRACE_AI')
            self.categorycode.overall_counts(df)

            #running bankwise counts method which gives bankwise counts
            df = self.dboperation.readTableIntodf('TRACE_AI')
            self.categorycode.bankwise_counts(df)

            #running channelwise counts method which gives channelwise counts
            self.categorycode.channelwise_counts(df)




        except Exception as e:
            self.log_writer.log(self.file_object, 'Model training unsuccessful')
            self.log_writer.log(self.file_object, 'Exception occurred in model training. Exception message is : '+ str(e))
            raise e