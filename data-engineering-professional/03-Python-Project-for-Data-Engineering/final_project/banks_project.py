import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    html = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)
    table = html.findAll("tbody")
    rows = table[0].findAll("tr")
    for row in rows:
        col = row.findAll("td")
        if len(col) != 0:
            name = col[1].text.strip()
            # print(col[1].findAll("a")[1]['title'])
            market_cap = float(col[2].text.strip())
            # print(float(col[2].contents[0]))
            data_dict = {"Name": name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df_fx = pd.read_csv(csv_path)
    dict = df_fx.set_index('Currency').to_dict()['Rate']
    # df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    # df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]
    # df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    for currency in dict.keys():
        df[f"MC_{currency}_Billion"] = round(df["MC_USD_Billion"] * dict[currency], 2)
    return df
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

db_name = "Banks.db"
table_name = "Largest_banks"
table_attrs = ["Name", "MC_USD_Billion"]
csv_path = "./Largest_banks_data.csv"

log_progress('Preliminaries complete. Initiating ETL process')

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
df = extract(url, table_attrs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, "./exchange_rate.csv")

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Print the contents of the entire table
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)
log_progress('Query 1 Complete.')

# Print the average market capitalization of all the banks in Billion USD
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)
log_progress('Query 2 Complete.')

# Print only the names of the top 5 banks
query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Query 3 Complete.')

log_progress('Process Complete.')

sql_connection.close()