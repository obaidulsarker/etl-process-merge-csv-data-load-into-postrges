# from asyncio import read
#from tkinter.ttk import Separator
from logging import exception
import psycopg2
from IPython.display import display
import pandas as pd
import numpy as np

# import necessary libraries
import os
import glob
import shutil

# Import data into DB
from sqlalchemy import create_engine, false
from sqlalchemy.sql import text as sa_text

# Database Connection
conn_file_path = "D:\python-project\etl\project\etl\config\db_connection.ini"
conn_file = open(conn_file_path, mode="r")
conn_string = conn_file.read()
conn_file.close()

# Output File
output_file = "D:\python-project\etl\project\etl\data\others\missing_value.csv"

# DB
db = create_engine(conn_string)

v_sql = """ SELECT CONCAT(tbl.logger_id,'_', tbl.fromdatetime)  AS missing_values
FROM (
SELECT TO_CHAR(t.fromdatetime,'yyyymmdd_HH24MI') AS fromdatetime, 
	TO_CHAR(t.todatetime,'yyyymmdd_HH24MI') AS todatetime, 
	s.logger_id,
SUM(CASE WHEN s.tstamp is not null THEN 1 ELSE 0 END) AS record_count,
(120 - SUM(CASE WHEN s.tstamp is not null THEN 1 ELSE 0 END)) As missing_records
FROM (
SELECT ts.generate_series AS fromdatetime, ts.generate_series + interval '2 hour' AS todatetime
FROM (
SELECT * from generate_series(
'2023-02-23 00:00',
'2023-02-23 23:59', INTERVAL '2 hour'
)
) ts
) t
LEFT JOIN public.measurements_v1 AS s
ON ( s.tstamp >= t.fromdatetime AND s.tstamp <t.todatetime) 
GROUP BY logger_id, t.fromdatetime, t.todatetime
) tbl
WHERE tbl.logger_id IS NOT NULL
AND tbl.missing_records>0 """

try:
    # Open connection
    conn = db.connect()

    # execute sql
    df=pd.read_sql(v_sql,conn)
    conn.close()

    # Output
    df.to_csv(output_file, index=False, header=False)
    
except Exception as e:
		conn.close()
		print(e.__str__)

print("DONE")
