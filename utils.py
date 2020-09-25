from csv import reader
import progressbar
import subprocess
import psycopg2
import pyodbc
def progress(btach_size):
    bar = progressbar.ProgressBar(maxval=btach_size,widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    return bar

def generate_ddl(cur,schema,table):
    list_str = []
    column_detail_sql = """SELECT column_name,data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'""".format(table)
    cur.execute(column_detail_sql)
    column_detail = cur.fetchall()
    column_dict = {}
    for a in column_detail:
        column_dict[a[0]] = a[1]
    for k,v in column_dict.items():
        str = """"{}" {}""".format(k,v)
        list_str.append(str)
    columns =  ",".join(list_str)
    create_str = """CREATE TABLE {}.{} ({});""".format(schema,table,columns)
    return  create_str

def save_out(schema,table,cur,sep):
    io = open(table+'.dat', 'w')
    cur.copy_to(io, schema+'.'+table, sep=sep)
    io.close()
def batch_insert(table,btach_size,column_detail,odbc_conn,odbc_cur, delm):
    bar = progress(btach_size)
    l = []
    values = []
    for c in column_detail:
        string = "[{}]".format(c[0])
        l.append(string)
        values.append('?')
    columns = ",".join(l)
    values = ",".join(values)
    insrt_str = "INSERT INTO dbo.{}({}) values({})".format(table, columns, values)
    print(insrt_str)
    i = 0
    with open(table+'.csv', 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj, delimiter=delm)
        bar.start()
        batch = []
        n=0
        for each in csv_reader:

            #print(i)
            i = i + 1
            bar.update(i)
            odbc_cur.execute(insrt_str, tuple(each))
            if i==btach_size:
                print("executing batch")
                odbc_conn.commit()
                batch = []
                n=n+i
                print("batch commited at: ", str(n)+" Rows")
                i = 0
                bar.finish()


def bcp_bulk_load(table, csv_file, server, database, user, password):
    str = "bcp {} in {}v -S {} -d {} -U {} -P {} -q -c -t  ,".format(table, csv_file, server, database, user, password)
    str.format(table, csv_file, server, database, user, password)
    print(str)
    out = subprocess.run(str, shell=True)

def get_psycopg_cursor(host, port, dbname, user, password ):
    conn = psycopg2.connect(host="{}".format(host), port="{}".format(port), dbname="{}".format(dbname), user="{}".format(user), password="{}".format(password))
    return conn

def get_odbc_cursor(DRIVER, Server, DATABASE, UID, PWD):
    conn_str = 'DRIVER={};Server={};DATABASE={};UID={};PWD={}'.format(DRIVER, Server, DATABASE, UID, PWD)
    print(conn_str)
    cnxn = pyodbc.connect(conn_str)
    return cnxn

def get_columndetail(cur, table):
    column_detail_sql = """SELECT column_name,data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'""".format(table)
    cur.execute(column_detail_sql)
    column_detail = cur.fetchall()
    return column_detail

def verify(pg_cursor,odbc_cur, source_schema, source_table, dest_schema, dest_table):
    pg_cursor.execute("SELECT COUNT(*) FROM {}.{}".format(source_schema, source_table))
    odbc_cur.execute("SELECT COUNT(*) FROM {}.{}".format(dest_schema,dest_table))
    source_count = pg_cursor.fetchall()
    dest_count = odbc_cur.fetchall()
    return source_count, dest_count