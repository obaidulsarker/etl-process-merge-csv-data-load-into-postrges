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
conn_file = open("etl\config\db_connection.ini", mode="r")
conn_string = conn_file.read()
conn_file.close()

db = create_engine(conn_string)
table_name = "soda_15"


# use glob to get all the csv files 
source_path = os.getcwd() + "\\etl\\data\\others"
destination_path = os.getcwd() + "\\etl\\archive\\15min"

# read only csv files
csv_files = glob.glob(os.path.join(source_path, "*.csv"))

# loop over the list of csv files
for f in csv_files:

	file_name = f.split("\\")[-1]

	print ("Read CSV file: ", file_name)

	# read the csv for latitude and longitude
	df = pd.read_csv(f, sep=',', header=0)

	df["tstamp"] = pd.to_datetime(df['tstamp'], format='%Y-%m-%d %H:%M:%S')

	#Load data into staging
	print("Loading data ..............................")
	try:
		# Open Connection
		conn = db.connect()

		# Load data into staging
		df.to_sql("measurements_v1", con=conn, schema='public', if_exists='replace', index=False)
		conn.autocommit = True

		conn.close()

	except Exception as e:
		conn.close()
		print(e.__str__)

	print("DONE ..............................")

print()
