import psycopg2
from datetime import datetime
import time
import pandas as pd
import os

# Database Credentials
PG_SERVER="localhost"
DB_PORT= '5432'
DB_NAME="etl"
USER_NAME="postgres"
DB_PASSWORD="12345"

# CSV file location
csv_data_dir = "D:\python-project\etl\project\etl\data\kharsaa"
csv_data ="D:\python-project\etl\project\etl\data\kharsaa\Kha_min2.csv"

def check_file_existance():
    if os.path.exists(csv_data):
        return True
    else:
        print(f"CSV File does not exist in {csv_data}")
        return False


Struct = ('TIMESTAMP', 'GHI_Avg', 'Diffuse_Avg', 'DNI_Avg',
          'CHP1_BodyTemp_Avg', 'EKOML01_Avg', 'AirTC_Avg', 'RH_Avg',
          'NANML01_Tot', 'BP_mbar_Avg',	'WS_ms_Avg', 'WS_ms_Max',
          'WS_ms_TMx', 'WS_ms_Min',	'WindDir', 'WindDir_Avg')

# ------->
# Get date of last updated measurement
def get_pr_stamps():
    with db_connect().cursor() as curs:
        curs.execute('''
            select tstamp from measurments_kharsaa
                order by tstamp desc'''
                      )
        last_times = curs.fetchall()  # All processed timestamps
    return [pr_time[0] for pr_time in last_times ]


# ------->
# Get not recently updated data from csv
def get_new(df):
    prevs_stamps = get_pr_stamps()
    latest = [m for m in df if len(m[0]) > 0 and
              m[0] not in prevs_stamps]
    return latest


# ------->
# Read CSV file with measument's data
def read_kha():
    #rows = open('E:\KharsaaData\Kha_min.csv').read().split('\n')
    rows = open(csv_data).read().split('\n')
    measurements = [rows[ii].split(',')
                    for ii in range(4, len(rows))]

    for m in range(len(measurements) -1):
        del measurements[m][1]  # Unused column
        for jj in enumerate(measurements[m]):
            if jj[0] != 0 and jj[0] != 12:  # to skip timestamp columns
                try:
                    measurements[m][jj[0]] = float(
                        measurements[m][jj[0]])
                except: measurements[m][jj[0]] = float()  # in case of NAN presents
            else:
                measurements[m][jj[0]] = measurements[m][jj[0]].replace(
                    '"', '')
    return measurements


# ------->
# Write data to db table
def write2table(new_ones):
    with db_connect().cursor() as curs:
        for new_rec in new_ones:
            try:
                curs.execute('''
                    insert into public.measurments_kharsaa VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, 
                    %s, %s,%s, %s, %s, %s)''',
                    new_rec)
            except:
                pass
    return None

def db_connect():
    _conn = psycopg2.connect(
        host=PG_SERVER,
        database=DB_NAME,
        user=USER_NAME,
		port= DB_PORT,
        password=DB_PASSWORD)
    _conn.autocommit = True
    return _conn


def create_Kha_table():
    with db_connect().cursor() as curs:
        curs.execute('''CREATE TABLE if not exists 
            public.measurments_kharsaa
            (
            tstamp timestamp without time zone NOT NULL,
            ghi_avg real,
            diffuse_avg real,
            dni_avg real,
            chp1_bodytemp_avg real,
            ekoml01_avg real,
           airtc_avg real,
           rh_avg real,
            nanml01_tot real,
            bp_mbar_avg real,
            ws_ms_avg real,
            ws_ms_max real,
            ws_ms_tmx time without time zone,
            ws_ms_min real,
            winddir real,
            winddir_avg real)''')
    return None

def create_temp_table():
    with db_connect().cursor() as curs:
        curs.execute('''CREATE TABLE if not exists 
            public.temp_table
            (
            tstamp timestamp without time zone NOT NULL,
            ghi_avg real,
            diffuse_avg real,
            dni_avg real,
            chp1_bodytemp_avg real,
            ekoml01_avg real,
           airtc_avg real,
           rh_avg real,
            nanml01_tot real,
            bp_mbar_avg real,
            ws_ms_avg real,
            ws_ms_max real,
            ws_ms_tmx time without time zone,
            ws_ms_min real,
            winddir real,
            winddir_avg real)''')
    return None

def read_insert_kha_data():
    
    # read and parse the data
    #data = pd.read_csv('E:\KharsaaData\Kha_min.csv', header=3)
    data = pd.read_csv(csv_data, header=3)
    
    data = data.drop(data.columns[1], axis=1) # drop second column
    

    data.columns = [
        'tstamp', 'ghi_avg', 'diffuse_avg', 'dni_avg', 'chp1_bodytemp_avg', 
        'ekoml01_avg', 'airtc_avg', 'rh_avg', 'nanml01_tot', 
        'bp_mbar_avg', 'ws_ms_avg', 'ws_ms_max', 'ws_ms_tmx', 'ws_ms_min', 
        'winddir', 'winddir_avg']
    
    print(data.info())

    print(data.head(5))

    csv_tmp_file_save = f"{csv_data_dir}\\temp.csv"
    data.to_csv(csv_tmp_file_save, mode='w', index=False)
    
    time.sleep(10)

    # inser the parsed data into the temp_table
    with db_connect().cursor() as curs:
        curs.execute(f'''
                     COPY public.temp_table
                        FROM '{csv_tmp_file_save}'
                        DELIMITER ','
                        CSV HEADER;''')
                        
    return None

def main():
    create_Kha_table()
    # all_df = read_kha()☻
    # new_ones = get_new(all_df)
    while True:
        if check_file_existance() is False:
            break

        if datetime.now().minute == 10 and datetime.now().second == 0:   
            time.sleep(10)

            with db_connect().cursor() as curs:
                curs.execute('''DROP TABLE IF EXISTS public.temp_table;''')
            
            # create temp table
            create_temp_table()
            
            # read the Al-Kharsaah data and insert it in the temp_table
            read_insert_kha_data()
            
            # insert the tem_table data into measurments_kharsaa avoiding duplicates
            with db_connect().cursor() as curs:
                curs.execute('''
                INSERT INTO public.measurments_kharsaa
                SELECT * 
                FROM public.temp_table a 
                 WHERE NOT EXISTS ( SELECT 0 FROM public.measurments_kharsaa b WHERE b.tstamp = a.tstamp )
                             ''')
            print("Data is imported into database successfully!")

            time.sleep(3300) # wait 55 minutes

    return None


if __name__ == '__main__':
    main()
    
    
    
    
    
    
    
    
    
    
    