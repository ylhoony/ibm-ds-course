# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root', password='MjQ5MTgteWxob29u',host='127.0.0.1',database='sales')

# Connect to DB2 or PostgreSql
# connectction details
dsn_hostname = '127.0.0.1'
dsn_user='postgres'             # e.g. "abc12345"
dsn_pwd ='MTEwNTcteWxob29u'     # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"        # i.e. "BLUDB"

# create connection
psql_conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)
psql_cursor = psql_conn.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    psql_cursor = psql_conn.cursor()
    psql_cursor.execute("SELECT MAX(rowid) FROM products;")
    row = psql_cursor.fetchone()
    row_id = row[0]
    psql_cursor.close()
    return row_id


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"SELECT * FROM products WHERE rowid > {rowid}")
    rows = mysql_cursor.fetchall()
    mysql_cursor.close()
    return rows

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    psql_cursor = psql_conn.cursor()
    insert_query ="INSERT INTO products(rowid,product,category) values(%s,%s,%s)" 
    for row in records:
        psql_cursor.execute(insert_query ,row)
        psql_conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_conn.close()

# disconnect from DB2 or PostgreSql data warehouse 
psql_conn.close()

# End of program