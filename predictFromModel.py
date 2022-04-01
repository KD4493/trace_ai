import pandas as pd
from file_operations import file_methods

from application_logging import logger
from Database_operations.dboperation import dbOperation
import glob
from trainingModel import trainModel
import pickle
from pickle import dump, load
from sklearn.preprocessing import StandardScaler


class prediction:

    def __init__(self):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.dboperation = dbOperation(self.file_object, self.log_writer)
        self.bankfilepath = glob.glob(r'Forecasted_dataframes/Bank_Counts' + "/*.csv")
        self.chanelpath = glob.glob(r'Forecasted_dataframes/Channel_Counts' + "/*.csv")

    def predictionFromModel(self):

        try:
            overall_df = pd.read_csv('Forecasted_dataframes/Overall_Counts/overallcounts.csv')
            bdfs = []
            for filename in self.bankfilepath:
                bdfs.append(pd.read_csv(filename))
            bank_df = pd.concat(bdfs, ignore_index=True )

            cdfs = []
            for filename in self.chanelpath:
                cdfs.append(pd.read_csv(filename))
            chanel_df = pd.concat(cdfs, ignore_index=True)




            #bank1_df = pd.read_csv('Forecasted_dataframes/Bank_Counts/bank1_counts.csv')
            #bank1_df['Bank_Code'] = 1
            #bank2_df = pd.read_csv('Forecasted_dataframes/Bank_Counts/bank2_counts.csv')
            #bank2_df['Bank_Code'] = 2
            # channel1_df = pd.read_csv('Forecasted_dataframes/Channel_Counts/channel1_counts.csv')
            # channel1_df['Channel'] = 1
            # channel2_df = pd.read_csv('Forecasted_dataframes/Channel_Counts/channel2_counts.csv')
            # channel2_df['Channel'] = 2
            df_temp = overall_df[['ds','unmatch_count','matched_count','rev_count','unsuccess_count','total_count']][-38:]
            self.dboperation.insertdftoSQL(df_temp, 'Trend_Prediction_Overall')
            df_bank = bank_df[['ds','Bank_Code','unmatch_count','matched_count','rev_count','unsuccess_count','total_count']][-38:]
            self.dboperation.insertdftoSQL(df_bank, 'Trend_Prediction_Bank')
            # df_bank2 = bank2_df[['ds','Bank_Code','unmatch_count','matched_count','rev_count','unsuccess_count','total_count']][-38:]
            # self.dboperation.insertdftoSQL(df_bank2, 'Trend_Prediction_Bank')
            df_chanel = chanel_df[['ds','Channel','unmatch_count','matched_count','rev_count','unsuccess_count','total_count']][-38:]
            self.dboperation.insertdftoSQL(df_chanel, 'Trend_Prediction_Chanel')
            # df_chanel2 = channel2_df[
            #     ['ds','Channel', 'unmatch_count', 'matched_count', 'rev_count', 'unsuccess_count', 'total_count']][-15:]
            # # self.dboperation.insertdftoSQL(df_chanel2, 'Trend_Prediction_Chanel')
            self.log_writer.log(self.file_object, 'Predictions data inserted to Database successfully')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
