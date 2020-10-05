'''import jaydebeapi as jay
import os
import psycopg2
import datetime

import pyodbc
from bcpandas import SqlCreds,to_sql'''
import os
import utils

#from sqlalchemy import MetaData, create_engine

# local gp server details
host = "localhost"
port = "5432"
dbname = "postgres"
user= "aritra"
password= "Nq1dRuaV"
# source file(dump)
# odbc Server Details
DRIVER= "{ODBC Driver 17 for SQL Server}"
Server= "gptoazure.database.windows.net"
Port = "5432"
DATABASE= "star"
UID= "aritra"
PWD= "Nq1dRuaV"


'''# creating dump
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
cur = pg_conn.cursor()
utils.save_out('public','star_data',cur,",")'''

'''# partition data
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
cur = pg_conn.cursor()
name_of_table = "combined_file"
chunk_size = 1000000
utils.save_part('star_data.csv', chunk_size, 'csv')'''

'''# bcp
import pandas as pd
target = {}
target["target_count"] = []
target["source_file"] = []
target["STATUS"] = []
conn, str = utils.get_odbc_cursor(DRIVER, Server, Port, DATABASE, UID, PWD)
cur = conn.cursor()
data_info = pd.read_csv("chunks/star_data_chunk_info.csv")
files = data_info['File_name']
folder = '/home/aritra/PycharmProjects/Greenplum_to_Azure/'
load_count = 0
for each in files:
    try:
        utils.bcp_bulk_load("star_data3", folder+each, Server, DATABASE, UID, PWD)
    except:
        target["STATUS"].append("FAILED")
        exit(1)
    cur.execute("SELECT COUNT(*) FROM dbo.star_data3")
    target_count = cur.fetchall()
    load_count = int(target_count[0][0]) - load_count
    target["target_count"].append(load_count)
    target["source_file"].append(folder+each)
    if target_count == load_count:
        target["STATUS"].append("SUCCESS")
    else:
        target["STATUS"].append("FAILED")
    target_df = pd.DataFrame.from_dict(target, orient='index').transpose()
    target_df.to_csv("chunks/target_info.csv")'''



'''# batch load
odbc_conn = utils.get_odbc_cursor(DRIVER, Server, DATABASE, UID, PWD)
odbc_cur = odbc_conn.cursor()
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
pg_cursor = pg_conn.cursor()
btach_size = 1000
column_detail = utils.get_columndetail(pg_cursor, "star_data")
utils.batch_insert("star_experiment",btach_size,column_detail,odbc_conn,odbc_cur, ',')'''




'''# generate ddl
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
pg_cursor = pg_conn.cursor()
ddl = utils.generate_ddl(pg_cursor,'public','star_data')
print(ddl)'''

'''
odbc_conn = utils.get_odbc_cursor(DRIVER, Server, DATABASE, UID, PWD)
odbc_cur = odbc_conn.cursor()
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
pg_cursor = pg_conn.cursor()
sc,dc = utils.verify(pg_cursor,odbc_cur, 'public', 'star_data', 'dbo', 'star_experiment')
print(sc,dc)
'''

'''import pandas as pd
import pandas.io.sql as pdsql
df = pd.read_csv("star_experiment.csv")
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
name_of_table = "star_data"
chunk_size = 1000000
offset = 0
chunk_num = 0
id = ""
bar = utils.progress(15857625)
bar.start()
while True:
  sql = "SELECT ctid,* FROM %s  order by ctid limit %d offset %d " % (name_of_table, chunk_size, offset)
  the_frame = pdsql.read_sql_query(sql, pg_conn)
  the_frame.to_parquet('chunks/'+name_of_table+'_'+str(chunk_num)+'.parquet',compression='UNCOMPRESSED')
  offset += chunk_size
  bar.update(offset)
  chunk_num += 1
bar.finish()'''




'''# to_sql
from sqlalchemy import event
import sqlalchemy
import urllib
import pandas as pd
conn, str = utils.get_odbc_cursor(DRIVER, Server, Port, DATABASE, UID, PWD)
db_params = urllib.parse.quote_plus(str)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
df = pd.read_csv("chunks/star_datapart_0.csv")
cursor = engine.connect()
cursor.fast_executemany = True
df.to_sql("star_experiment",engine,index=False,if_exists="append",schema="dbo")
'''
'''import pandas as pd
import csv
import time
import sys
start_time = time.time()
conn = utils.get_psycopg_cursor(host, port,dbname, user, password)'''
'''cur = conn.cursor()'''
'''format = "csv"
name_of_table = "star_data"
chunk_num = 0
chunk_size = 50000000
schema = "public"
name_of_table = "star_data"
bar = utils.progress(chunk_size)
bar_val = 0
bar.start()
chunk_size_sql = 3000000
with conn.cursor(name='custom_cursor') as cursor:
    cursor.itersize = chunk_size_sql # chunk size
    query = 'SELECT * FROM {}.{};'.format(schema, name_of_table )
    cursor.execute(query)
    chunk_num=0
    i=0
    dict_df = {}
    dict_df["File_name"] = []
    dict_df["num_rows"] = []
    csv_file = 0
    for row in cursor:
        #rowl = [str(a) for a in row]
        chunk_num = chunk_num + 1
        if chunk_num <= chunk_size:
            if not csv_file:
                csv_file = open('chunks/' + name_of_table + '_part_' + str(i) + '.csv', 'a')
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(row)
            bar.update(chunk_num)
        else:
            print(" {} saved in {}".format(('chunks/' + name_of_table + '_part_' + str(i) + '.csv'),
                                          (time.time() - start_time)))
            dict_df["num_rows"].append(chunk_num)
            dict_df["File_name"].append('chunks/' + name_of_table + '_part_' + str(i) + '.csv')
            i = i+1
            csv_file.close()
            csv_file = 0
            chunk_num = 0
            bar.finish()
bar.finish()
df_nfo = pd.DataFrame.from_dict(dict_df,orient='index').transpose()
df_nfo.to_csv("chunks/{}_info.csv".format(name_of_table))'''




import pandas as pd
import csv
import time
import sys
import multiprocessing
start_time = time.time()
#conn = utils.get_psycopg_cursor(host, port,dbname, user, password)
name_of_table = "star_data"
chunk_num = 0
chunk_size = 50000000
schema = "public"
name_of_table1 = "star_data"
name_of_table2 = "star_data2"
bar_val = 0
chunk_size_sql = 3000000

#tables = utils.get_tables(conn, schema)
tables = [name_of_table1, name_of_table2]
connections = {}
for tab in tables:
    connections[tab] = utils.get_psycopg_cursor(host, port,dbname, user, password)
proc = []
for table in tables:
    conn = connections[table]
    tab = multiprocessing.Process(target=utils.save_parts,
                                   args=[conn, schema, table, chunk_size_sql, chunk_size])
    tab.start()
    proc.append(tab)
for process in proc:
    process.join()

#utils.load_blob()










