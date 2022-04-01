from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
import numpy as np


class Data_PreparationPred:
    """
            This class shall be used for data preparation from raw prediction data.

            Written by: Max_AI
            Version : 1.0
            Revisions: None
    """

    def init(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.raw_data = "PredictionFile/InputFile.csv"
        self.prepared_data = "Prepared_Data/Prepreparedfile.csv"


    def dataPreparationPred(self):
        """
                Method Name: dataPreparation
                Description: This method first reads the raw data provided by Monitoring team.
                             And according to our need this method prepares useful data

                 Written By: Max_AI
                 Version: 1.0
                 Revisions: None
        """

        try:
            df = pd.read_csv(self.raw_data)
            df_a = df[['TimeStamp','Cash Out', 'Cash loading']]
            df_a.replace({"Cash Out":     {"Machine dispensing cash": 0, "Cashout": 1},
                "Cash loading": {"Not loaded": 0, "Cash loaded": 1}})
            df_a = df_a.set_index('TimeStamp').resample('D').max().reset_index()
            df_b = df[['TimeStamp', 'Total Balance']]
            df_b['Cash Withdrawl'] = df_b['Total Balance'].diff(-1)
            df_b['Cash Withdrawl'] = np.where(df_b['Cash Withdrawl'] < 0, 0, df_b['Cash Withdrawl'])
            df_b = df_b.set_index('TimeStamp').resample('D').sum().reset_index()
            df_merged = pd.merge(df_a, df_b, on='TimeStamp', how='outer')
            df_merged.to_csv(self.prepared_data)
            self.logger_object.log(self.file_object, "Training file prepared from raw data successfully and saved to given location")
        except Exception as e:
            self.logger_object.log(self.file_object, "Data preparation failed!!! Exception message: %s" % e)
            raise e
