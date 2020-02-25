"""
Module Name: k_data_profile.py
Purpose: Create Data profiling reports for exploratory Data Analysis 
Created data: Jan, 2020
Author: Sophia Yue
Functionality:
    1.Connect Python to Snowflake with URL = 'abs_itds_dev.east-us-2.azure'
    2.Retrieve all the tables in EDM_CONFIRMED_DEV.DW_C_PRODUCT
    3.Ignore table with suffix of _COPY, _TEST, _TMP, _WRK, _FlAT, _TEMP
    4.Retrieve data from the tables in step2 and feed the data to profile_report and save the report to a html file
    5.Provide error handling   

Instruction:
 - Base on your need to change the values of  the dictionary including credential information 
   -  Use Safeway username and password for LDAP   
 - You can run the code via Spyder, Jupyter, 'python k_get_cnt.py' from command line    
 - Provide the capability to handle specific table(s) or batch to get all the tables 
   - For batch mode, from "Enter table name(s")command line,hit 'enter'
     Otherwise, enter the table name(s) and seperate by ','
 - Provide the capability to enter the path name for report files
   - Hit enter for default path 
Notes:
    - Install snowflake-connector-python
      - pip install --upgrade snowflake-connector-python
    - All the common codes are in OneDrive

""" 



import pandas as pd
import pandas_profiling
import datetime
from datetime import datetime, timedelta, date
import time 
import snowflake.connector
import os
from os import path
import logging


""" 
Step 1: Get common codes 
"""

prg_name = ""
path_code = "C:\\Users\\syue003\\wip_RecSys\\"
c_timedte = path_code + "c_time_dte.py" 
exec(compile(open(c_timedte, 'rb').read(),c_timedte, 'exec'))
"""Get common functions to connect Python to Snowflake """
c_sf_com_fnc =  path_code + "c_sf_com_fnc.py"

exec(compile(open(c_sf_com_fnc, 'rb').read(),c_sf_com_fnc, 'exec'))


""" 
Step 2: Connect Python to Snowflake
 - Define dictionary for connect setting
 - Invoke cf_sf_connect to set connection 

"""


d_secrets = {
    "account"   : "abs_itds_dev.east-us-2.azure",
    "role"      : "EDDM_DATA_READER_GG", 
    "user"      : "syue003@safeway.com",
    "warehouse" : "LOAD_WH",            
    "database"  : "EDM_CONFIRMED_DEV",
    "schema"    : "DW_C_PRODUCT",
    "password"  : "Chungli$1"
    
  }

db_nm  = d_secrets["database"]
sch_nm = d_secrets["schema"]


connection = cf_sf_connect (d_secrets)


""" 
Step 3: Enter Table name(s)
  - If table name is not '' 
    - Will strip leading and trailing spaces and convert table name to upper case
  - If table name is '', will use batch mode 
    - Build a list to get table names under database EDM_CONFIRMED_DEV with schema  DW_C_PRODUCT
      - Ignore table with suffix  of  _COPY, _TEST, _TMP, _WRK, _FLAT,  _TEMP, or _TST 
"""

l_tbl_nm  = list(map(str, input("Hit 'Enter' for batch mode or enter table name(s); Use ',' as a seperator:").split(','))) 
l_tbl_nm = [tbl_nm.upper().strip()  for tbl_nm in l_tbl_nm ]

if l_tbl_nm [0] == '':
    q_tbl_nm = f"""SELECT table_name  FROM {db_nm}.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{sch_nm}' 
      and  table_name not like '%_COPY'  and  table_name not like  '%_TEST' 
      and  table_name not like '%_TMP'   and  table_name not like  '%_WRK%'
      and  table_name not like '%_FLAT'  and  table_name not like  '%_TEMP'
      and  table_name not like '%_TST'
      group by table_name order by table_name"""
     
    df_q_tbl_nm = cf_cr_df_cur_qry(q_tbl_nm)
    
    """ 
    - Create a dataframe from the q_tbl_nm qyery result 
    - Create a list of tbl_nm and flatten the list
    """
    df_q_tbl_nm = cf_cr_df_cur_qry(q_tbl_nm)
    l_tbl_1 =  df_q_tbl_nm.values.tolist() # list with list: [['BANNER'], ['BOD_LAST_RUN_STATUS'], ...., ['ZZ_TST_ECAT']]
    l_tbl_nm = [item for sublist in l_tbl_1 for item in sublist] # list: ['BANNER', 'BOD_LAST_RUN_STATUS', ...., 'ZZ_TST_ECAT']
   

""" Step 4: Enter path for output
    If path name is '', will use default path
    otherwise, will create a folder if folder does not exist
"""

output_path = str(input("Hit 'Enter' for default path or enter the path name:"))
if output_path == '':
    output_path = 'C:\\SYUE\\EDM\\Data_profiling_report\\'
else:
  if not os.path.exists(output_path):
     os.mkdir(output_path)    

""" 
Step 5: Build a dictionary to save count from all the tables been selected
- If the table does not exist, will cause ProgrammingError
  - To use 'except ProgrammingError:' 
    - Need to import ProgrammingError from pyobdc 
      - from pyodbc import ProgrammingError
    - The system still print system error message     
  - Use generic error message instead   
    -  except Exception 
    -  logging.exception
"""

cnt_lmt_x = 20
cnt_lmt_y = 100000 

d_tbl_cnt = {}
for tbl_nm in l_tbl_nm:  
    try:
       q_cnt= f"select count(*) from {db_nm}.{sch_nm}.{tbl_nm}"
       cur_q_cnt  = connection.cursor().execute(q_cnt)  
       for row in cur_q_cnt:
           cnt = row[0]
           print (f'tbl_nm: {tbl_nm}, cnt: {cnt}')
           if cnt >  cnt_lmt_x:
              d_tbl_cnt[tbl_nm] = cnt
    except Exception:
          logging.exception(f'table {db_nm}.{sch_nm}.{tbl_nm} cause an error')         
                
           
""" 
Create profile reports
 - Will base on count of table and cnt_lmt_y to get smp_pct to sample the rows for creating profile report  
 - Table CONSUMER_WARNING will cause integer converting to floating overflow error
   - Will exclude the table 
 -  For a table with count > closed to 1,000,000, it'd take more than 2 hours to complete the profile report 

"""

for tbl_nm, cnt in d_tbl_cnt.items():
    print ('tbl_name', tbl_nm, 'cnt', cnt, 'cnt_lmt_y', cnt_lmt_y  )
    smp_pct = 100.00
    try:
      if cnt >  cnt_lmt_y :
           print('big') 
           smp_pct = round(cnt_lmt_y/cnt * 100, 0) 
      print(f'table_name: {tbl_nm}, smp_pct = {smp_pct}')
      s_col = cf_get_col_nm(tbl_nm)
      q_sel_data = f'select {s_col}  from EDM_CONFIRMED_DEV.DW_C_PRODUCT.{tbl_nm} tablesample bernoulli ({smp_pct})'
      file_nm_low = tbl_nm.lower()
      int_smp_pct = int(smp_pct)
      file_nm = f'{tbl_nm}_{int_smp_pct}pct.html'
      df_q_sel_data = cf_cr_df_cur_qry(q_sel_data) 
      cf_cr_data_profile_rpt(df_q_sel_data, file_nm)
    except Exception as ex:
        logging.exception(f'table {tbl_nm} cause an error')                   
        


