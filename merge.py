import pandas as pd
import os

class Merge():

    def __init__(self):
        self.db_project_df = pd.DataFrame()

    def merge_project_files(self,Lonic, db):
        """

        :param Lonic: pandas datefrane
        :param db: pandas datefrane
        :return: merge pandas dataframe
        """

        result = pd.merge(Lonic, db, how='right',  right_on='LOINC-NUM',left_on="LOINC_NUM")
        return result

    def replase_project_files(self,db, path,lonic):
        """

        :param db: pandas datefrane
        :param path: path of the new data
        :param lonic: andas datefrane
        :return:
        """
        if path is not None:
            df = pd.read_excel(path)
            result = self.merge_project_files(lonic,df)
        else:
            result = self.merge_project_files(lonic, db)
        result.to_excel("Data/project_db_test_publish.xlsx")
        return result

    def add_data(self, data_to_add):
        global db_project_df
        df_loinc = pd.read_excel(os.path.join("Data/project_db_test_publish.xlsx"))
        df_loinc = df_loinc.append(data_to_add)
        df_loinc.to_excel("Data/project_db_test_publish.xlsx")
        return df_loinc

    def retrieve(self, first_name, last_name, transac_date,transac_time, start_date, start_time, comp_or_loinc, lonic_comp_val):
        """

        :param first_name: first name of the patient
        :param last_name: last name of the patient
        :param transac_date: transaction date
        :param transac_time:transaction time
        :param start_date: start date as string format
        :param start_time:start time as string format
        :param comp_or_loinc: boolean value. comp = 1 loinc =0. bool value to identify the filed we need to update
        :return: dataframe or null in case non of the parameters filtered
        """
        import datetime as datetime
        # with time case
        if transac_time is not None:
            datetime_transaction_str = transac_date + " " + transac_time
        # no transaction time
        else:
            datetime_transaction_str = transac_date + " " + "00:00:00"
        datetime_object = datetime.datetime.strptime(datetime_transaction_str, '%d/%m/%Y %H:%M:%S')

        res = self.db_project_df.loc[(self.db_project_df['First name'] == first_name) & (self.db_project_df['Last name'] == last_name)
                          & (self.db_project_df['Transaction time'] <= datetime_object)]

        if start_time is None:
            start_time = "00:00:01"
        datetime_start_str = start_date + " " + start_time
        datetime_start_object = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
        res = res[res['Valid start time'] >= datetime_start_object]

        if comp_or_loinc == 'comp':
            res = res.loc[res['COMPONENT'] == lonic_comp_val]
        else:
            res = res.loc[res['LOINC-NUM'] == lonic_comp_val]

        if res is not None:
            if res.shape[0] > 1:
                res = res.sort_values(by=["Transaction time", "Valid start time"], ascending=False).head(1)

        return res



    def history(self, logic_num, first_name, last_name,transac_date,transac_time, start_date, end_date, start_time, end_time):
        """

        :param logic_num:
        :param fiest_name: first name of the patient
        :param last_name: last name of the patient
        :param start_date: start date to the search
        :param end_date: end date to the search
        :param start_time: start time to search
        :param end_time: end time to search
        :return: pandas dataframe
        """
        import datetime as datetime
        # with time case
        if transac_time is not None:
            datetime_transaction_str = transac_date + " " + transac_time
        # no transaction time
        else:
            datetime_transaction_str = transac_date + " " + "00:00:00"
        datetime_transaction_obj = datetime.datetime.strptime(datetime_transaction_str, '%d/%m/%Y %H:%M:%S')

        if start_time is None:
            start_time = "00:00:01"
        if end_time is None:
            end_time = "23:59:00"

        datetime_start_str = start_date + " " + start_time
        datetime_end_str = end_date + " " + end_time

        datetime_start_obj = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
        datetime_end_obj = datetime.datetime.strptime(datetime_end_str, '%d/%m/%Y %H:%M:%S')

        tmp_db = db_project_df.loc[db_project_df['Transaction time'] <= datetime_transaction_obj]
        tmp_db = tmp_db.loc[(tmp_db['Valid start time'] >= datetime_start_obj) &
                                   (tmp_db['Valid stop time'] <= datetime_end_obj)]
        tmp_db = tmp_db.loc[(tmp_db['LOINC-NUM'] == logic_num) & (tmp_db['First name'] == first_name) &
                                   (tmp_db['Last name'] == last_name)]
        return tmp_db

    def update(self, update_date, updat_time, comp_or_loinc_val, first_name, last_name, new_date, new_time, new_value):
        """
        :param update_date: the date we will update
        :param updat_time: the time we will update
        :param comp_or_loinc: 0 or 1. comp = 1 loinc =0. bool value to identify the filed we need to update
        :param first_name: first name of the patient
        :param last_name: last name
        :param new_date: this is the new vlaue of "Valid stop time"  and the "Transaction time" date filed
        :param new_time: this is the new vlaue of "Valid stop time" and the "Transaction time" date time filed
        :param new_value: The updated value
        :return: the updated row. if no row updated return null
        """
        import datetime as datetime
        global db_project_df
        datetime_start_str = update_date + " " + updat_time
        datetime_start_obj = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
        tmp_db = self.db_project_df.loc[(self.db_project_df['Valid start time'] == datetime_start_obj) &
                                   (self.db_project_df['First name'] == first_name) &
                                   (self.db_project_df['Last name'] == last_name) &
                                   ((self.db_project_df['LOINC-NUM'] == comp_or_loinc_val) | (
                                               self.db_project_df['COMPONENT'] == comp_or_loinc_val))
                                   ]
        if tmp_db.empty:
            return None
        # more then one row
        if tmp_db.shape[0] > 1:
            row_to_update = tmp_db.sort_values(by="Transaction time", ascending=False).head(1).copy()
            old_value = row_to_update.copy()
        # only one row return
        else:
            row_to_update = tmp_db
            old_value = row_to_update.copy()

        row_to_update["Value"] = new_value

        datetime_new_str = new_date + " " + new_time
        datetime_new_obj = datetime.datetime.strptime(datetime_new_str, '%d/%m/%Y %H:%M:%S')
        row_to_update["Valid start time"] = old_value["Valid start time"]
        row_to_update["Valid stop time"] = old_value["Valid stop time"]
        row_to_update["Transaction time"] = datetime_new_obj
        self.db_project_df = self.db_project_df.append(row_to_update)
        return [row_to_update, old_value]

    def delete(self, tran_date, tran_time, comp_or_loinc, first_name, last_name, del_date, del_time):
        import datetime as datetime

        datetime_start_str = del_date + " " + del_time
        datetime_start_obj = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
        tmp_db = db_project_df.loc[(self.db_project_df['Valid start time'] <= datetime_start_obj) &
                                   (self.db_project_df['First name'] == first_name) &
                                   (self.db_project_df['Last name'] == last_name) &
                                   (self.db_project_df["LOINC-NUM"] == comp_or_loinc | self.db_project_df["COMPONENT"] == comp_or_loinc)]

        if tmp_db.empty:
            return None
        #more then one row
        if tmp_db.shape[0] > 1:
            row_to_update = tmp_db.sort_values(by="Transaction time", ascending=False).head(1).copy()
            old_value = row_to_update.copy()
        #only one row return
        else :
            row_to_update = tmp_db
        row_to_update["Value"] = None

        datetime_new_str = tran_date + " " + tran_time
        datetime_new_obj = datetime.datetime.strptime(datetime_new_str, '%d/%m/%Y %H:%M:%S')
        row_to_update["Transaction time"] = datetime_new_obj

        self.db_project_df = self.db_project_df.append(row_to_update)
        return [row_to_update,old_value]

    def main(self):
        path = "Data"
        global db_project_df
        df_loinc = pd.read_csv(os.path.join(path, "Loinc.csv") )

        db_df = pd.read_excel(os.path.join(path, "project_db_test_publish.xlsx") )
        db_project_df = self.merge_project_files(df_loinc, db_df)
        db_project_df["Valid start time"] = pd.to_datetime(db_project_df["Valid start time"])
        db_project_df["Valid stop time"] = pd.to_datetime(db_project_df["Valid stop time"])
        db_project_df["Transaction time"] = pd.to_datetime(db_project_df["Transaction time"])
        res_retive = self.retrieve("Eyal", "Rothman", "21/05/2018", "10:00:00", "18/05/2018", "15:00:00", True)
        res_history1 = self.history("12181-4","Yonathan","Spoon", "20/05/2018", "10:00:00","20/05/2018","20/05/2018","02:09:00",
                              None)
        res_history2 = self.history("14743-9","David","Mizrahi", "22/05/2018 ", "10:00:00","19/05/2018","19/05/2018","03:03:00",
                              "03:03:00")
        res_update = self.update("20/05/2018 ", "02:09:00",True,"Eli","Call","30/08/2020", "12:30:00", "500000-55")
        res_update_null = self.update("20/05/2019 ", "02:09:00",True,"Eli","Call","30/08/2020", "12:30:00", "500000-55")
        print("retrive: ")
        print(res_retive)
        print("history 1 ")
        print(res_history1)
        print("history 2 ")
        print(res_history2)
        print("update ")
        print(res_update)
        print("update null")
        print(res_update_null)





