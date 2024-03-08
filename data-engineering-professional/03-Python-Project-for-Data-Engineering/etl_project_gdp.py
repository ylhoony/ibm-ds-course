import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import os

src_url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# check whether directory already exists
# def download_src(path, src_url, file_name):
#   src_path = f"{path}/{file_name}"
#   if not os.path.exists(path):
#       os.mkdir(path)
#       print("Folder %s created!" % path)
#   else:
#       print("Folder %s already exists" % path)

#   if not glob.glob(src_path):
#       wget.download(src_url, out=src_path)

# download_src(path, src_url1, "INSTRUCTOR.csv")


path = './prtc_projects'
db_name = f'{path}/World_Economies.db'
table_name = 'Countries_by_GDP'
table_attrs = ["Country", "GDP_USD_millions"]
csv_path = f'{path}/Countries_by_GDP.csv'

# Code for ETL operations on Country-GDP data

# Importing the required libraries


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(src_url).text
    HTML = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns=table_attrs)
    table = HTML.findAll("tbody")
    rows = table[2].findAll("tr")
    for row in rows[3:]:
        cols = row.findAll("td")

        if len(cols)!=0:
            if cols[0].find('a') is None:
                continue
            if cols[2].text == "—":
                continue
            
            data_dict = {"Country": cols[0].a.text, "GDP_USD_millions": cols[2].text}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df["GDP_USD_millions"] = df["GDP_USD_millions"].apply(lambda x: float(x.replace(",", "")))
    df["GDP_USD_millions"] = round(df["GDP_USD_millions"] / 1000, 2)
    df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"}, inplace=True)
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(f"{path}/log_file.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(src_url, table_attrs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()