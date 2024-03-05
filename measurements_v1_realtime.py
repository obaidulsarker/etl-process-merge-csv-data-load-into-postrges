from logging import exception
from IPython.display import display

# Import data into DB
from sqlalchemy import create_engine, false
from sqlalchemy.sql import text as sa_text

# Database Connection
conn_file = open("etl\config\db_connection.ini", mode="r")
conn_string = conn_file.read()
conn_file.close()

db = create_engine(conn_string)
table_name = "measurements_v1_realtime"

# Read SQL file
sql_file = open("etl\sql\measurements_v1_realtime_ins.sql", mode="r")
sql_ins = sql_file.read()
sql_file.close()

#Load data into staging
print("Loading data ..............................")
try:
		# Open Connection
		conn = db.connect()

		# Truncate table
		print("Trauncating data ..............................")
		conn.execute(sa_text('''TRUNCATE TABLE public.%s''' % (table_name)).execution_options(autocommit=True))

		# load data into prod from staging
		print("Loading data ..............................")
		conn.execute(sa_text(sql_ins).execution_options(autocommit=True))

		conn.close()

except Exception as e:
		conn.close()
		print(e.__str__)

print("DONE ..............................")

print()