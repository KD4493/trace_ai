"""
    This file should be used for executing different tasks like running prophet on overall counts, running prophet on bank
    wise counts, running prophet on channel wise counts.
"""

import pandas as pd

class category_codes:

    def __init__(self, file_object, logger_object, tracemethod):
        self.file_object = file_object
        self.logger_object = logger_object
        self.tracem = tracemethod


    def overall_counts(self, df):
        df.drop(columns=['Bank_Code', 'Channel'], inplace=True)
        df = df.groupby(by='Transaction_date').sum()
        df.reset_index(inplace=True)
        matched_count = self.tracem.matched_prophet(df)
        unmatched_count = self.tracem.unmatch_prophet(df)
        rev_count = self.tracem.rev_prophet(df)
        unsuccess_count = self.tracem.unsuccess_prophet(df)
        total_count = self.tracem.total_prophet(df)

        overall_df = pd.concat([matched_count, unsuccess_count, rev_count, unmatched_count, total_count], axis=1)

        overall_df = overall_df.loc[:, ~overall_df.columns.duplicated()]

        overall_df.to_csv('Forecasted_dataframes/Overall_Counts/overallcounts.csv')

    def bankwise_counts(self, df):
        unique_banks = df['Bank_Code'].unique()
        for i in unique_banks:
            df_ = df[df['Bank_Code']== i]
            df_.drop(columns=['Bank_Code', 'Channel'], inplace=True)
            df_ = df_.groupby(by='Transaction_date').sum()
            df_.reset_index(inplace=True)
            matched_count = self.tracem.matched_prophet(df_)
            unmatched_count = self.tracem.unmatch_prophet(df_)
            rev_count = self.tracem.rev_prophet(df_)
            unsuccess_count = self.tracem.unsuccess_prophet(df_)
            total_count = self.tracem.total_prophet(df_)

            bankwise_df = pd.concat([matched_count, unsuccess_count, rev_count, unmatched_count, total_count], axis=1)
            bankwise_df['Bank_Code'] = i

            bankwise_df = bankwise_df.loc[:, ~bankwise_df.columns.duplicated()]

            bankwise_df.to_csv(f'Forecasted_dataframes/Bank_Counts/bank{i}_counts.csv')

    def channelwise_counts(self, df):
        unique_channels = df['Channel'].unique()
        for i in unique_channels:
            df_ = df[df['Channel'] == i]
            df_.drop(columns=['Bank_Code', 'Channel'], inplace=True)
            df_ = df_.groupby(by='Transaction_date').sum()
            df_.reset_index(inplace=True)
            matched_count = self.tracem.matched_prophet(df_)
            unmatched_count = self.tracem.unmatch_prophet(df_)
            rev_count = self.tracem.rev_prophet(df_)
            unsuccess_count = self.tracem.unsuccess_prophet(df_)
            total_count = self.tracem.total_prophet(df_)

            channelwise_df = pd.concat([matched_count, unsuccess_count, rev_count, unmatched_count, total_count],
                                    axis=1)
            channelwise_df['Channel'] = i
            channelwise_df = channelwise_df.loc[:, ~channelwise_df.columns.duplicated()]

            channelwise_df.to_csv(f'Forecasted_dataframes/Channel_Counts/channel{i}_counts.csv')









