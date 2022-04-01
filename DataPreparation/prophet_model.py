import pandas as pd
import datetime as dt
from fbprophet import Prophet
from fbprophet.plot import add_changepoints_to_plot
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics
from fbprophet.plot import plot_cross_validation_metric


class prophet:
    """
                This class shall be used to train prophet model on prepared data.
                Further we shall divide data into training data and prediction data for
                classification model, we will do this in this class.

                Written By: Kuldeep
                Version: 1.0
                Revisions: None
    """
    def __init__(self,file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def add_holiday_effect(self):
        """
                Method Name: add_holiday_effect
                Description: This method makes a dataframe called playoff having two columns namely
                             ds and holiday.
                Output: Playoff Dataframe

                On failure: Raise exception
        """
        self.logger_object.log(self.file_object, 'Entered the add_holiday_effect mehtod of prophet class')
        try:
            playoffs = pd.DataFrame({
                'holiday': 'playoff',
                'ds': pd.to_datetime(['2019-09-02', '2019-09-03', '2019-09-04',
                                      '2019-09-05', '2010-09-06', '2019-10-08',
                                      '2019-10-09', '2019-10-10', '2019-10-11',
                                      '2019-10-12', '2019-10-13', '2019-10-14',
                                      '2019-10-15', '2019-10-16', '2019-10-17', '2019-10-25', '2019-10-26',
                                      '2019-10-27', '2019-10-28',
                                      '2019-10-29', '2019-10-16', '2021-09-10', '2021-09-11', '2021-09-12',
                                      '2021-09-13', '2021-09-14', '2021-09-15', '2021-09-16',
                                      '2021-09-10', '2021-10-15', '2021-10-16', '2021-10-17',
                                      '2021-10-18', '2021-10-19', '2021-10-20', '2021-10-21',
                                      '2021-10-22', '2021-10-23', '2021-10-24', '2021-11-01',
                                      '2021-11-02', '2021-11-03', ]),
                'lower_window': -1,
                'upper_window': 0,
            })
            return playoffs
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in add_holiday_effect method. Exception message: ',str(e))
            self.logger_object.log(self.file_object, 'Adding holiday effect unsuccessful. Exited the add_holiday_effect method of prophet class')
            raise e

    def fit_prophet_model(self, df):
        """
                Method Name: prophet_model
                Description: This mehtod takes dataframe, playoff dataframe and regressor features and fits
                             prophet model.
                Output: a prophet model
                On failure: raise exception

                Written By: Max_AI
                Version: 1.0
                Revisions: None

        """
        self.logger_object.log(self.file_object, 'Entered the fit_prophet_model of prophet class')
        self.df = df
        # self.playof = playof
        # self.reg_1 = reg_1
        # self.reg_2 = reg_2
        # self.reg_3 = reg_3
        # self.reg_4 = reg_4
        # self.reg_5 = reg_5
        try:
            m = Prophet(yearly_seasonality=False, daily_seasonality=True)
            # m.add_regressor(self.reg_1)
            # m.add_regressor(self.reg_2)
            # m.add_regressor(self.reg_3)
            # m.add_regressor(self.reg_4)
            # m.add_regressor(self.reg_5)
            m.fit(df)
            return m
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in fit_prophet_model of prophet class. Exception message: ',str(e))
            self.logger_object.log(self.file_object, 'fit_prophet_model method unsuccessful. Exited the fit_prophet_model of prophet class')
            raise e

    def make_future_df(self, m):
        """
                Method Name: make_future_df
                Description: This method takes fitted prophet model and return future dataframe for period of 30 days.
                Output: future column
                On failure: Raise Exception

                Written By: Kuldeep
                Version: 1.0
                Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the make_future_df method of prophet class')
        self.m = m
        try:
            future = m.make_future_dataframe(periods=38)
            return future
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in make_future_df mehtod of prophet class.Exception message: ',str(e))
            self.logger_object.log(self.file_object, 'make_future_df unsuccessful. Exited make_future_df mehtod of prophet class')
            raise e

    def forecast_future(self, m, future_df):
        """
                Method Name: forecast_future
                Description: This method make forecast on prepared future datafarame.
                Output: forecast dataframe
                On failure: Raise Exception

                Written By: Kuldeep
                Version: 1.0
                Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the forecast_future mehtod of prophet class')
        self.m = m
        self.future_df = future_df
        try:
            forecast = m.predict(self.future_df)
            return forecast
        except Exception as e:
            self.logger_object(self.file_object, 'Exception occured in forecast_future method. Exception message: ',str(e))
            self.logger_object(self.file_object, 'Future forecast unsuccessful. Exited forecast_future method of prophet class')
            raise e







