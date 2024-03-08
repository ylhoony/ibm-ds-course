# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv

# Python Scripting: Database initiation
import sqlite3
import pandas as pd
import os
import glob
import wget

# specify the path for the directory
path = './db_code'
src_url1 = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv"
src_url2 = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv"

# check whether directory already exists
def download_src(path, src_url, file_name):
  src_path = f"{path}/{file_name}"
  if not os.path.exists(path):
      os.mkdir(path)
      print("Folder %s created!" % path)
  else:
      print("Folder %s already exists" % path)

  if not glob.glob(src_path):
      wget.download(src_url, out=src_path)

download_src(path, src_url1, "INSTRUCTOR.csv")
download_src(path, src_url2, "Departments.csv")

# Connect to the SQLite3 service
conn = sqlite3.connect(f'{path}/STAFF.db')

# Define table parameters
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

table_name2 = 'Departments'
attribute_list2 = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']

# Read the CSV file
file_path = f'{path}/{table_name}.csv'
df = pd.read_csv(file_path, names=attribute_list)

file_path2 = f'{path}/{table_name2}.csv'
df2 = pd.read_csv(file_path2, names=attribute_list2)

# Load the data to a table
df.to_sql(table_name, conn, if_exists='replace', index=False)
df2.to_sql(table_name2, conn, if_exists='replace', index=False)
print('Table is ready')


# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Define data to be appended
data_dict = {'ID': [100],
            'FNAME': ['John'],
            'LNAME': ['Doe'],
            'CITY': ['Paris'],
            'CCODE': ['FR']}
data_append = pd.DataFrame(data_dict)

# Append data to the table
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Define data to be appended
data_dict2 = {'DEPT_ID': [9],
            'DEP_NAME': ['Quality Assurance'],
            'MANAGER_ID': [30010],
            'LOC_ID': ['L0010']}
data_append2 = pd.DataFrame(data_dict2)

# Append data to the table
data_append2.to_sql(table_name2, conn, if_exists='append', index=False)
print('Data appended successfully')

# a. View all entries
query_statement = f"SELECT * FROM {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# b. View only the department names
query_statement = f"SELECT DISTINCT DEP_NAME FROM {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# c. Count the total entries
query_statement = f"SELECT COUNT(*) FROM {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# Close the connection
conn.close()
