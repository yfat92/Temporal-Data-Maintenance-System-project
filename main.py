import merge
import datetime
import pandas as pd
import os

merge_c = merge.Merge()
path = "Data"
db_project_df = merge_c.db_project_df
df_loinc = pd.read_csv(os.path.join(path, "Loinc.csv"))

db_df = pd.read_excel(os.path.join(path, "project_db_test_publish.xlsx"))
merge_c.db_project_df = merge_c.merge_project_files(df_loinc, db_df)
merge_c.db_project_df["Valid start time"] = pd.to_datetime(merge_c.db_project_df["Valid start time"])
merge_c.db_project_df["Valid stop time"] = pd.to_datetime(merge_c.db_project_df["Valid stop time"])
merge_c.db_project_df["Transaction time"] = pd.to_datetime(merge_c.db_project_df["Transaction time"])
'''Displays menu for user and runs program according to user commands.'''
prompt_main = """
Select one of the following options: 
    1. Choose current date. 
    2. Retrieve. 
    3. Retrieve History. 
    4. Update
    5. Delete
    6. Replace data
    7. Add data
    8. Quit.\nEnter your selection: """
userInp = ""
run = True
cond1 = False
cond2 = False
date = datetime.date(2018, 12, 1)
while(run):

    userInp = input(prompt_main)

    if userInp == '1':
        date = input('Enter current date in the following format %d-%m-%y:')
        date = datetime.datetime.strptime(date, '%d/%m/%Y %H:%M:%S')
        print('Current date is :' + str(date))

    elif userInp == '2':
        first_name = input('Enter first_name:')
        last_name = input('Enter last_name:')
        transac_date = input('Enter current date (optional):')
        if transac_date == '':
            transac_date = date
        transac_time = input('Enter current time (optional):')
        if transac_time == '':
            transac_time = None
        start_date = input('Enter start date:')
        start_time = input('Enter start time (optional):')
        if start_time == '':
            start_time = None
        comp_or_loinc = input('Choose "comp" for comp or "lonic" for lonic:')
        comp_or_loinc_val = input('Choose comp or lonic value:')
        ans = merge_c.retrieve(first_name, last_name, transac_date,transac_time, start_date, start_time, comp_or_loinc,comp_or_loinc_val)
        print(ans.to_string())


    elif userInp == '3':
        logic_num = input('Enter logic_num:')
        first_name = input('Enter first_name:')
        last_name = input('Enter last_name:')
        valid_start_date = input('Enter valid start date:')
        valid_start_time = input('Enter valid start time (optional):')
        if valid_start_time == '':
            valid_start_time = None
        trans_start_date = input('Enter trans start date:')
        trans_start_time = input('Enter trans start time (optional):')
        if trans_start_time == '':
            trans_start_time = None
        trans_end_date = input('Enter trans end date:')
        trans_end_time = input('Enter trans end time (optional):')
        if trans_end_time == '':
            trans_end_time = None
        ans = merge_c.history(logic_num, first_name, last_name,valid_start_date,valid_start_time, trans_start_date, trans_start_time, trans_end_date, trans_end_time)
        if ans.empty:
            print('There is no measurement that matches the publication of The query')
        else:
            print(ans.to_string())


    elif userInp == '4':
        update_date = input('Enter Valid start date :')
        update_time = input('Enter Valid start time :')
        if update_time == '':
            update_time = None
        comp_or_loinc_val = input('Enter comp or lonic name current value:')
        first_name = input('Enter first_name:')
        last_name = input('Enter last_name:')
        new_date = input('Enter new date (new Transaction date):')
        new_time = input('Enter new time (new Transaction time):')
        new_value = input('Enter new value:')
        ans = merge_c.update(update_date, update_time, comp_or_loinc_val, first_name, last_name, new_date, new_time, new_value)
        if ans is None:
            print('There is no measurement that matches the publication of The query')
        else:
            print('The update was done')
            print(ans[0].to_string())
            print(ans[1].to_string())

    elif userInp == '5':
        tran_date = input('Enter transaction date:')
        tran_time = input('Enter transaction time:')
        comp_or_loinc_val = input('Enter comp or lonic value:')
        first_name = input('Enter first_name:')
        last_name = input('Enter last_name:')
        del_date = input('Enter del date:')
        del_time = input('Enter del time (optional) :')
        if del_time == '':
            del_time = None
        ans = merge_c.delete(tran_date, tran_time, comp_or_loinc_val, first_name, last_name, del_date, del_time)
        if ans is None:
            print('There is no measurement that matches the publication of The query')
        else:
            print('The update was done')
            print(ans[0].to_string())
            print(ans[1].to_string())
    elif userInp == '6':
        path = input('Enter new file path:')
        ans = merge_c.replase_project_files(merge.db_project_df, path, df_loinc)

    elif userInp == '7':
        path = input('Enter new file path:')
        df = pd.read_excel(path)
        ans = merge_c.add_data(df)
    else:
        error= ''
        if(not cond1):
            error = "Error: no data generated yet"#
        elif(not cond2):
            error = "Error: data generated but least squares not completed"
        print(error)
