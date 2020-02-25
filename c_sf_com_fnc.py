"""
Module Name: c_sf_com_fnc.py
Purpose: Commom modules to to connect Python and snowflake and access Snowflake tables
Created data: Jan, 2020
Author: Sophia Yue

"""
"""
The calling program need to import the following package
import pandas as pd
import pandas_profiling
import datetime
from datetime import datetime, timedelta, date
import time 
import snowflake.connector
import os
"""


"""
Module Name: cf_sf_connect
Purpose    : Connect Python to Snowflake
Parameter  : 
  p_dic    : Dictionary to define variables required to connection
  - The dictionary need to have the key of 'account, role, user, password,database, schema, and warehouse'
    - Example
      d_secrets = {
       "account"   : "abs_itds_dev.east-us-2.azure",
       "role"      : "EDDM_BI_ENGINEER_GG", 
       "user"      : "syue003@safeway.com",
       "warehouse" : "LOAD_WH",            
       "database"  : "EDM_CONFIRMED_DEV",
       "schema"    : "SCRATCH",
       "password"  : "Chungli$1" }
 Output
  - o_connection   : connection
   
"""
def cf_sf_connect(p_dic):
    import snowflake.connector
    connection = snowflake.connector.connect(
    account   = p_dic["account"],
    role      = p_dic["role"],
    user      = p_dic["user"],
    password  = p_dic["password"],
    database  = p_dic["database"],
    schema    = p_dic["schema"],
    warehouse = p_dic["warehouse"],
    authenticator = 'externalbrowser'
    )
    return connection


def cf_cr_df_cur_qry(p_qry):
    """
    Mudule name: cf_cr_df_cur_qry
    Purpose: Create a dataframe from the p_sql query result
    parameter 
     p_qry: sql
    Output
     a dataframe
    """
    cur_p_qry = connection.cursor().execute(p_qry)
    return  pd.DataFrame.from_records(iter(cur_p_qry), columns=[x[0] for x in cur_p_qry.description])

def cf_get_col_nm(p_tbl_nm):
     """
     Mudule name: cf_get_col_nm
     Purpose: out a string to have all the column name excluding the ones with DW_ prefix
     parameter 
      p_tbl_nm: Table name
     Output
      string with col names
     """    
 
     q_col = f'''SELECT COLUMN_NAME FROM {db_nm}.INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = '{sch_nm}'  
                and table_name = '{p_tbl_nm}' 
                and COLUMN_NAME not like 'DW_%' order by ORDINAL_POSITION'''    
                
     df_q_col = cf_cr_df_cur_qry(q_col) 
     l_col = df_q_col.values.tolist()
     l_col_flat = [item for sublist in df_q_col.values.tolist() for item in sublist]
     return  ','.join(l_col_flat)
 
def cf_cr_data_profile_rpt(p_df, p_file_nm):
    """
     Module name : cf_cr_data_profile_rpt
     parameter 
      p_df: Data frame to profile
      p_file_nm: file name for profile report 
     Output
      a html file
    """
    start_time = time.time() 
    print(f'process table:{p_file_nm}' )
    profile = p_df.profile_report(title='Data Profiling Report - ' +  p_file_nm  )
    profile.to_file(output_file= output_path + p_file_nm + ".html")
    end_time = time.time() 
    cf_elapse_time (start_time, end_time, dsc = f'Pandas Profiling Report for {p_file_nm }. \n')
    
    