import pandas as pd
import os

db_project_df = pd.DataFrame()

def merge_project_files(Lonic, db):
    """

    :param Lonic: pandas datefrane
    :param db: pandas datefrane
    :return: merge pandas dataframe
    """

    result = pd.merge(Lonic, db,how='right',  right_on='LOINC-NUM',left_on="LOINC_NUM")
    return  result


def retrieve(first_name, last_name, date, time):
    import datetime as datetime
    datetime_str = date + " " + time
    datetime_object = datetime.datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')

    res = db_project_df.loc[(db_project_df['First name'] == first_name) & (db_project_df['Last name'] == last_name)
                      & (db_project_df['Transaction time'] >= datetime_object)]

    if res is not None:
        if res.shape[0] > 1:
            res = res.sort_values(by="Transaction time", ascending=False)
            return res.head(1)
        else:
            return res
    return res



def history(logic_num, first_name, last_name,transac_date,transac_time, start_date, end_date, start_time, end_time):
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
    datetime_start_str = start_date + " " + start_time
    datetime_end_str = end_date + " " + end_time

    datetime_start_obj = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
    datetime_end_obj = datetime.datetime.strptime(datetime_end_str, '%d/%m/%Y %H:%M:%S')

    tmp_db = db_project_df.loc[db_project_df['Transaction time'] == datetime_transaction_obj]
    tmp_db = db_project_df.loc[(tmp_db['Valid start time'] >= datetime_start_obj) &
                               (tmp_db['Valid stop time'] <= datetime_end_obj)]
    tmp_db = db_project_df.loc[(tmp_db['LOINC-NUM'] == logic_num) & (tmp_db['First name'] == first_name) &
                               (tmp_db['Last name'] == last_name)]
    return tmp_db


def update (update_date, updat_time, comp_or_loinc, first_name, last_name, new_date, new_time, new_value):
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
    datetime_start_str = update_date + " " + updat_time
    datetime_start_obj = datetime.datetime.strptime(datetime_start_str, '%d/%m/%Y %H:%M:%S')
    tmp_db = db_project_df.loc[(db_project_df['Valid start time'] == datetime_start_obj) &
                               (db_project_df['Valid stop time'] == last_name) & (db_project_df['First name'] == first_name)]
    if tmp_db is None:
        return None
    #more then one row
    if tmp_db.shape[0] > 1:
        row_to_update = tmp_db.sort_values(by="Transaction time", ascending=False).head(1)
        old_value = row_to_update.copy()
    #only one row return
    else :
        row_to_update = tmp_db
    if comp_or_loinc == 1:
        row_to_update["LOINC-NUM"] = new_value
    else:
        row_to_update["COMPONENT"] = new_value

    datetime_new_str = new_date + " " + new_time
    datetime_new_obj = datetime.datetime.strptime(datetime_new_str, '%d/%m/%Y %H:%M:%S')
    row_to_update["Valid start time"] = datetime_new_obj
    row_to_update["Valid stop time"] = datetime_new_obj
    row_to_update["Transaction time"] = datetime_new_obj
    db_project_df.append(row_to_update)
    return [row_to_update,old_value]






def main():
    path = "Data"
    global db_project_df
    df_loinc = pd.read_csv(os.path.join(path, "Loinc.csv") )

    db_df = pd.read_excel(os.path.join(path, "project_db_test_publish.xlsx") )
    db_project_df = merge_project_files(df_loinc, db_df)
    db_project_df["Valid start time"] = pd.to_datetime(db_project_df["Valid start time"])
    db_project_df["Valid stop time"] = pd.to_datetime(db_project_df["Valid stop time"])
    db_project_df["Transaction time"] = pd.to_datetime(db_project_df["Transaction time"])
    res_retive = retrieve("Eyal", "Rothman", "17/05/2018", "21:00:00")

    print(res_retive)




if __name__ == "__main__":
    main()