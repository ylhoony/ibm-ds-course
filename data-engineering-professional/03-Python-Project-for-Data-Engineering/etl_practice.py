# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip
# unzip source.zip

import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import wget
import zipfile
import os

# specify the path for the directory
path = './etl_practice'

# check whether directory already exists
if not os.path.exists(path):
    os.mkdir(path)
    print("Folder %s created!" % path)
else:
    print("Folder %s already exists" % path)

src_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
src_path = f"{path}/datasource.zip"

if not glob.glob(src_path):
    wget.download(src_url, out=src_path)

with zipfile.ZipFile(src_path, 'r') as zip:
    zip.extractall(path)

log_file = f"{path}/log_file.txt"
target_file = f"{path}/transformed_data.csv"

# Task 1: Extraction


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = car.find("price").text
        fuel = car.find("fuel").text
        dataframe = pd.concat([dataframe, pd.DataFrame([{"car_model": car_model,
                                                         "year_of_manufacture": year_of_manufacture,
                                                         "price": price,
                                                         "fuel": fuel}])],
                              ignore_index=True)
    return dataframe


def extract():
    # create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # process all csv files
    for csvfile in glob.glob(f"{path}/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))],
                                   ignore_index=True)

    # process all json files
    for jsonfile in glob.glob(f"{path}/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))],
                                   ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob(f"{path}/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))],
                                   ignore_index=True)

    return extracted_data

# Task 2 - Transformation


def transform(data):
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['price'] = round(data.price, 2)

    return data

# Task 3 - Loading and Logging


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Testing ETL operations and log progress
# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
