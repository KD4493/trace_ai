import pandas as pd
import datetime as dt



class featureAddition:
    """
            This class shall be used to add features line weekend, salary days, festival days, monthly effects, daily effects
            cash loading pattern, cash_withdrawn_pattern etc.

            Written By: Max_AI
            Version: 1.0
            Revisions: None

    """
    def __init__(self,file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object


    def get_month_days(self, df):
        """
                Method Name: get_Months_Days
                Description: This method takes dataframe and runs functions to get months and days
                            from dates. Also it get dummies of the same.
                Output: Dataframe with Months and Days columns

                On failure: Raise Exception

                Written By: Max_AI
                Version : 1.0
                Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_Months_Days method of Feature_Addition class')
        try:
            df['Months'] = df.TimeStamp.apply(lambda x: dt.datetime.strptime(x, '%b')).tolist()
            df['Weekday'] = df.TimeStamp.apply(lambda x: dt.datetime.strptime(x, '%A')).tolist()
            df = pd.get_dummies(df, columns=['Months'])
            df = pd.get_dummies(df, columns=['Weekday'])
            return df
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in get_Months_Days method of Feature_Addition '
                                                     'class. Exception message:: ', str(e))
            self.logger_object.log(self.file_object, 'get_Months_Days unsuccessful. Exited get_Months_Days mehtod of '
                                                     'Feature_Addition class')
            raise e

    def get_weekends(self, column_name):
        """
                Method Name: get_Weekends
                Description: This method takes TimeStamp column and checks whether day is weekday or weekend
                Output: 1 for weekend and 0 for weekday
                On failure: Raise Exception

                Written By: Max_AI
                Version : 1.0
                Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_Weekends method of Feature_Addition class')
        try:
            date = pd.to_datetime(self.column_name)
            if date.weekday() == 5 or date.weekday() == 6:
                return 1
            else:
                return 0
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_Months_Days method of Feature_Addition class. Exception message:: ',str(e))
            self.logger_object.log(self.file_object,'get_Months_Days unsuccessful. Exited get_Months_Days mehtod of Feature_Addition class')
            raise e

    def get_salary_days(self, column_name):
        """
                Method Name: get_Salary_days
                Description: This method takes ds column and checks whether day is salary day or not
                Output: 1 for salary day and 0 for no salary day
                On failure: Raise Exception

                Written By: Kuldeep
                Version : 1.0
                Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_Salary_Days method of Feature_Addition class')
        try:
            date = pd.to_datetime(self.column_name)
            dtup = date.timetuple()
            if dtup.tm.mday <= 10:
                return 1
            else:
                return 0
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in get_salary_days mehtod of Feature_Addition '
                                                     'class')
            self.logger_object.log(self.file_object, 'get_salary_day unsuccessful. Exited get_salary_days method of '
                                                     'Feature_Addition class')
            raise e

    def get_month_end(self, column_name):
        """
                Method Name: get_month_end
                Description: This method takes ds column and checks whether day is month end  or not
                Output: 1 for month end and 0 else
                On failure: Raise Exception

                Written By: Max_AI
                Version : 1.0
                Revisions: None

        """
        self.logger_object.log(self.file_object, 'Entered the get_month_end method of feature_addition class')
        try:
            date = pd.to_datetime(self.column_name)
            dtup = date.timetuple()
            if dtup.tm.mday >= 25:
                return 1
            else:
                return 0
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occurred in get_month_end method. Exception message:', str(e))
            self.logger_object.log(self.file_object, 'Exited get_month_end method of featureAdditin class')
            raise e

    def get_month_start(self, column_name):
        """
                        Method Name: get_month_start
                        Description: This method takes ds column and checks whether day is month start  or not
                        Output: 1 for month start and 0 else
                        On failure: Raise Exception

                        Written By: Kuldeep
                        Version : 1.0
                        Revisions: None

                """
        self.logger_object.log(self.file_object, 'Entered the get_month_start method of featureAddition class')
        try:
            date = pd.to_datetime(self.column_name)
            dtup = date.timetuple()
            if dtup.tm.mday >= 1 and dtup.tm.mday <= 5:
                return 1
            else:
                return 0
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_month end method.Exception message: ',str(e))
            self.logger_object.log(self.file_object,'Exited the get_month_end method of Feature_Additon class')
            raise e

    def get_month_mid(self, column_name):
        """
                        Method Name: get_month_mid
                        Description: This method takes ds column and checks whether day is month mid  or not
                        Output: 1 for month mid and 0 else
                        On failure: Raise Exception

                        Written By: Kuldeep
                        Version : 1.0
                        Revisions: None

                """
        self.logger_object.log(self.file_object, 'Entered the get_month_mid method of feature_addition class')
        try:
            date = pd.to_datetime(self.column_name)
            dtup = date.timetuple()
            if dtup.tm.mday >= 10 and dtup.tm.mday <= 20:
                return 1
            else:
                return 0
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_month end method. Exception message: ',str(e))
            self.logger_object.log(self.file_object,'Exited the get_month_end method of featureAddition class')
            raise e

    def get_cash_load_pattern(self,cash_load_list):
        """
                    Method Name: get_cash_load_pattern
                    Description: This method makes pattern of cash loading.
                    Output: Dataframe containing cash load pattern
                    On Failure: Raise Exception

                    Written By: Kuldeep
                    Version: 1.0
                    Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_cash_load_pattern of feature_addition class')

        try:
            cash_load_list = [0.0, 0.0, 0.0] + cash_load_list
            cash_load_pattern = []
            for i in range(3, len(cash_load_list)):
                cash_load_pattern.append(cash_load_list[i-3, i+1])
            df_cash_load_pattern = pd.DataFrame(cash_load_pattern)
            self.logger_object.log(self.file_object, 'Cash load pattern successful. Exited the get_cash_load_pattern '
                                                     'method of feature_addition class')
            return df_cash_load_pattern
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in get_cash_load_pattern. Exception message: ',str(e))
            self.logger_object.log(self.file_object, 'Cash load pattern unsuccessful. Exited the '
                                                     'get_cash_load_pattern method of feature_addition class')
            raise e

    def get_cash_withdrawn_pattern(self, cash_withdrawn_list):
        """
                            Method Name: get_cash_withdrawn_pattern
                            Description: This method makes pattern of cash loading.
                            Output: Dataframe containing cash load pattern
                            On Failure: Raise Exception

                            Written By: Kuldeep
                            Version: 1.0
                            Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_cash_withdrawn_pattern of feature_addition class')

        try:
            cash_withdrawn_list = [0.0, 0.0, 0.0] + cash_withdrawn_list
            cash_withdrawn_pattern = []
            for i in range(3, len(cash_withdrawn_list)):
                cash_withdrawn_pattern.append(cash_withdrawn_list[i-3, i+1])
            df_cash_withdrawn_pattern = pd.DataFrame(cash_withdrawn_pattern)
            self.logger_object.log(self.file_object, 'Cash withdrawn pattern successful. Exited the '
                                                     'get_cash_withdrawn_pattern of feature_addition class')
            return df_cash_withdrawn_pattern
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in get_cash_withdrawn_pattern.Exception message: ', str(e))
            self.logger_object.log(self.file_object, 'Cash withdrawn pattern unsuccessful. Exited the '
                                                     'get_cash_withdrawn_pattern method of feature_addition class')
            raise e


