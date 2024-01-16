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
# D:\python-project\etl\project\etl\config\db_connection.ini
#base_path = "D:\python-project\etl\project"
conn_file_path = "D:\python-project\etl\project\etl\config\db_connection.ini"
conn_file = open(conn_file_path, mode="r")
#conn_file = open("etl\config\db_connection.ini", mode="r")
conn_string = conn_file.read()
conn_file.close()

db = create_engine(conn_string)
table_name = "historical_soda_15"

#Read SQL file
sql_file_imp = "D:\python-project\etl\project\etl\sql\historical_soda_15_ins.sql"
sql_file = open(sql_file_imp, mode="r")
sql_ins = sql_file.read()
sql_file.close()

# try:
# 	# Open Connection
# 	conn = db.connect()

# 	# Truncate table
# 	conn.execute(sa_text('''TRUNCATE TABLE public.%s''' % (table_name)).execution_options(autocommit=True))
# 	conn.close()
# except Exception as e:
# 	conn.close()
# 	print(e.__str__)

# use glob to get all the csv files 
source_path = "D:\\python-project\\etl\\project\\etl\\data\\realtime_soda_15"
destination_path ="D:\\python-project\\etl\\project\\etl\\archive\\historical_soda_15"

# source_path = os.getcwd() + "\\etl\\data\\realtime_soda_15"
# destination_path = os.getcwd() + "\\etl\\archive\\realtime_soda_15"

# read only csv files
csv_files = glob.glob(os.path.join(source_path, "*.csv"))

# loop over the list of csv files
for f in csv_files:

	file_name = f.split("\\")[-1]

	print ("Read CSV file: ", file_name)

	# read the csv for latitude and longitude
	df_lat_lon = pd.read_csv(f, sep=';', nrows=2, skiprows=4, names=['item','val'] )
	for column in df_lat_lon:
		if (column=='val') and (df_lat_lon[column].dtypes =='object'):
			df_lat_lon[column] = df_lat_lon[column].str.replace(r',', '')
			df_lat_lon[column] = df_lat_lon[column].astype('float')
	

	#print(df_lat_lon.info())

	# Get Latitude and Longitude
	latitude=df_lat_lon["val"].values[0]
	longitude=df_lat_lon["val"].values[1]

	#print("Latitude = ",latitude)
	#print("Longitude = ",longitude)

	# find inxex of the column header
	i = 0
	j = 0
	with open(f) as fi:
		for x in fi:
			i=i+1
			y = x.split(';')
			# check blank lines
			if x.split() == []:
				j = j + 1
			if len(y)>0:
				#print (y[0])
				if (y[0]=='# Date'):
					col_header_index=i
					break
			if (i>50):
				print('Column hearder is not found !!!')
				break
	
	print('Column header index = ',col_header_index)
	print('Blank Columns = ',j)

	# read the csv file for data
	df = pd.read_csv(f, header=[col_header_index-1-j], sep=';', skip_blank_lines=True)
	
	# Traverse columns
	for column in df:
		#print('Column name =', column)
		#print('column datatype=',df[column].dtypes)
		# Rename column
		if (column=='# Date'):
			df.rename(columns = {'# Date':'Date'}, inplace = True)
			column = 'Date'
		if (column=='Snow depth,,,,'):
			df.rename(columns = {'Snow depth,,,,':'Snow depth'}, inplace = True)
			column='Snow depth'
			df[column] = df[column].str.replace(r',', '').astype('float')

		# Change data type
		if ((column=='# Date') and (column=='Time'))==false:
			if (df[column].dtypes =='object'):
				df[column] = df[column].str.replace(r',', '')
				df[column] = df[column].astype(np.float16)
				#df[column] = df[column].astype('float')
		#if (column=='# Date'):
		#	df[column] = df[column].astype('str')

		# Update invalid time
		if (column=='Time'):
			df[column] = df[column].str.replace('24:00','23:59')
	
	#print(df.info())

	# computing number of rows and column
	rows = len(df.axes[0])
	cols = len(df.axes[1])

	#print("Number of Columns: ", cols)
	#print("Number of Rows: ", rows)
	
	# Remove null rows
	df.dropna()
	#print(df.info())

	# Add latitude and longitude
	df = df.assign(Latitude=latitude).round(3)
	df = df.assign(Longitude=longitude).round(3)

	# Convert Date and Time into Timestamp
	df_ts = pd.to_datetime(df['Date'].astype(str) + ' ' +df['Time'].astype(str))
	#display(df_ts)

	df.insert(2, 'datetime', df_ts)
	#print(df.info())

	# Drop "Date" and "Time" column
	df = df.drop(columns=['Date', 'Time'])

	#print(df.info())
	#display(df.head())

	#Load data into staging
	print("Loading data ..............................")
	try:
		
		# Open Connection
		conn = db.connect()

		# Truncate table
		conn.execute(sa_text('''TRUNCATE TABLE stg.%s''' % (table_name)).execution_options(autocommit=True))

		# delete all rows with column 'Age' has value 30 to 40
		#indexAge = df[ (df['Code'] == 6) ].index
		#df.drop(indexAge , inplace=True)
		#df.head(15)

		# Load data into staging
		df.to_sql(table_name, con=conn, schema='stg', if_exists='append', index=False)
		conn.autocommit = True

		# load data into prod from staging
		conn.execute(sa_text(sql_ins).execution_options(autocommit=True))

		conn.close()

		src_file=source_path+"\\"+file_name
		dest_file=destination_path+"\\"+file_name

		# move csv file to archive_data location
		shutil.move(src_file, dest_file, copy_function=shutil.copy2)

	except Exception as e:
		conn.close()
		print(e.__str__)

	print("DONE ..............................")

print()
