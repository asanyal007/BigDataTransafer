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
csv_file = "/home/aritra/PycharmProjects/Greenplum_to_Azure/chunks/star_datapart_1.cs"
# odbc Server Details
DRIVER= "{ODBC Driver 17 for SQL Server}"
Server= "gptoazure.database.windows.net"
DATABASE= "star"
UID= "aritra"
PWD= "Nq1dRuaV"


'''# creating dump
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
cur = pg_conn.cursor()
utils.save_out('public','star_data',cur,",")'''

# bcp
utils.bcp_bulk_load("star_experiment", csv_file, Server, DATABASE, UID, PWD)

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

'''# partition data
import pandas as pd
pg_conn = utils.get_psycopg_cursor(host, port, dbname, user, password )
cur = pg_conn.cursor()
#utils.save_out('public','star_data',cur,",")
name_of_table = "star_data"
chunk_size = 1000000
utils.save_part('star_data', chunk_size, 'csv')'''


# to_sql









