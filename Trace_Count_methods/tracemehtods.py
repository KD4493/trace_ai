"""
    This file is for running training methods on unmatched, matched, reversal, unsuccessful & total counts
"""
import pandas as pd





class tracemethod:

    def __init__(self, file_object, logger_object, prophet):
        self.file_object = file_object
        self.logger_object = logger_object
        self.prophet = prophet

    def matched_prophet(self, df):
        df_Match = df[['Transaction_date', 'Matched_Count']]
        df_Match['Transaction_date'] = pd.to_datetime(df_Match['Transaction_date'])
        df_Match.rename(columns={'Transaction_date': 'ds', 'Matched_Count': 'y'}, inplace=True)

        model = self.prophet.fit_prophet_model(df_Match)

        future = self.prophet.make_future_df(model)

        forecast = self.prophet.forecast_future(model, future)

        matched_count = forecast[['ds','yhat']]

        matched_count.rename(columns={'yhat' : 'matched_count'},inplace=True)

        return matched_count

    def unmatch_prophet(self, df):
        df_unmatch = df[['Transaction_date', 'Unmatched_Count']]
        df_unmatch['Transaction_date'] = pd.to_datetime(df_unmatch['Transaction_date'])
        df_unmatch.rename(columns={'Transaction_date': 'ds', 'Unmatched_Count': 'y'}, inplace=True)

        model = self.prophet.fit_prophet_model(df_unmatch)

        future = self.prophet.make_future_df(model)

        forecast = self.prophet.forecast_future(model, future)

        unmatch_count = forecast[['ds','yhat']]

        unmatch_count.rename(columns={'yhat': 'unmatch_count'}, inplace=True)

        return unmatch_count

    def rev_prophet(self, df):
        df_rev = df[['Transaction_date', 'Reversal_Count']]
        df_rev['Transaction_date'] = pd.to_datetime(df_rev['Transaction_date'])
        df_rev.rename(columns={'Transaction_date': 'ds', 'Reversal_Count': 'y'}, inplace=True)

        model = self.prophet.fit_prophet_model(df_rev)

        future = self.prophet.make_future_df(model)

        forecast = self.prophet.forecast_future(model, future)

        rev_count = forecast[['ds','yhat']]

        rev_count.rename(columns={'yhat': 'rev_count'},inplace=True)



        return rev_count

    def unsuccess_prophet(self, df):
        df_unsuccess = df[['Transaction_date', 'UnSuccessful_count']]
        df_unsuccess['Transaction_date'] = pd.to_datetime(df_unsuccess['Transaction_date'])
        df_unsuccess.rename(columns={'Transaction_date': 'ds', 'UnSuccessful_count': 'y'}, inplace=True)

        model = self.prophet.fit_prophet_model(df_unsuccess)

        future = self.prophet.make_future_df(model)

        forecast = self.prophet.forecast_future(model, future)

        unsuccess_count = forecast[['ds','yhat']]

        unsuccess_count.rename(columns={'yhat': 'unsuccess_count'},inplace=True)

        return unsuccess_count

    def total_prophet(self, df):
        df_total = df[['Transaction_date', 'Total_Count']]
        df_total['Transaction_date'] = pd.to_datetime(df_total['Transaction_date'])
        df_total.rename(columns={'Transaction_date': 'ds', 'Total_Count': 'y'}, inplace=True)

        model = self.prophet.fit_prophet_model(df_total)

        future = self.prophet.make_future_df(model)

        forecast = self.prophet.forecast_future(model, future)

        total_count = forecast[['ds','yhat']]

        total_count.rename(columns={'yhat': 'total_count'},inplace=True)

        return total_count