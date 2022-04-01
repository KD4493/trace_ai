from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
import numpy as np


class Data_Preparation:
    """
            This class shall be used for data preparing form raw data provided.

            Written by: Max_AI
            Version : 1.0
            Revisions: None
    """
    def __init__(self, filepath, file_object, logger_object, prepared_data):
        self.raw_data = filepath
        self.file_object = file_object
        self.logger_object = logger_object
        self.prepared_data = prepared_data


    def dataPreparation(self):
        """
                Method Name: dataPreparation
                Description: This method first reads the combined input.csv file and sample it daywise.

                 Written By: Max_AI
                 Version: 1.0
                 Revisions: None
        """

        try:
            df = pd.read_csv(self.raw_data, parse_dates=['TimeStamp'])
            df['Cash Out'].replace(['Machine dispensing cash', 'Cashout'], ['Machine Dispensing Cash', 'Cash Out'],
                                   inplace=True)     #we need to either automate this or catch it in data validation
            df['Cash loading'].replace(['Not loaded', 'Cash loaded'], ['Not Loaded', 'Cash Loaded'], inplace=True)
            df_a = df[['TimeStamp','Terminalid', ]]
            df_a = df_a.set_index('TimeStamp').resample('D').mode()
            df_a = df[['Terminalid','TimeStamp','Cash Out', 'Cash loading']]
            df_a.replace({"Cash Out":     {"Machine Dispensing Cash": 0, "Cash Out": 1},
                "Cash loading": {"Not Loaded": 0, "Cash Loaded": 1}}, inplace=True)
            df_a['Cash loading'].fillna(0, inplace=True)
            df_a = df_a.set_index('TimeStamp').resample('D').max().reset_index()

            df_b = df[['TimeStamp', 'Total Balance']]
            df_b['Total Balance'].fillna(method='bfill', inplace=True)
            df_b['Cash Withdrawl'] = df_b['Total Balance'].diff(-1)
            df_b['Cash Withdrawl'] = np.where(df_b['Cash Withdrawl'] < 0, 0, df_b['Cash Withdrawl'])
            df_b = df_b.set_index('TimeStamp').resample('D').sum().reset_index()
            df_merged = pd.merge(df_a, df_b, on='TimeStamp', how='outer')
            df_merged.to_csv(self.prepared_data)
            self.logger_object.log(self.file_object, "Training file prepared from raw data successfully and saved to given location")
        except Exception as e:
            self.logger_object.log(self.file_object, "Data preparation failed!!! Exception message: %s" % e)
            raise e
