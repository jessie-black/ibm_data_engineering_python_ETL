# Import libraries
#!mamba install pandas==1.3.3 -y
#!mamba install requests==2.26.0 -y
import glob
import pandas as pd
from datetime import datetime

# download dataset
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv

# define function to create pandas dataframe from json file
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = False)
    return dataframe

# EXTRACT
def extract():
    extracted_data = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)']) # empty dataframe w/column headings
    extracted_data = extracted_data.append(extract_from_json("bank_market_cap_1.json"), ignore_index=True)
    return extracted_data

## Question 1 Load the file exchange_rates.csv as a dataframe and find the exchange rate for British pounds with the symbol GBP, store it in the variable exchange_rate, you will be asked for the number. Hint: set the parameter index_col to 0.
csvdata = pd.read_csv("exchange_rates.csv",index_col =0)
exchange_rate = csvdata.loc['GBP']
 # 0.732398

# TRANSFORM
def transform(data):
    data['Market Cap (US$ Billion)'] = round(extracted_data['Market Cap (US$ Billion)'] * float(exchange_rate),3)
    data.rename(columns = {'Market Cap (US$ Billion)':'Market Cap (GBP$ Billion)'}, inplace=True)
    return data

# LOAD
def load(data_to_load):
    data_to_load.to_csv("bank_market_cap_gbp.csv", index=False)

# LOG 
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

# RUNNING ETL PROCESS
log("ETL Job Started")
log("Extract phase Started")
extracted_data = extract()
extracted_data.head()
log("Extract phase Ended")
log("Transform phase Started")
transformed_data = transform(extracted_data)
transformed_data.head()
log("Transform phase Ended")
log("Load phase Started")
load(transformed_data)
log("Load phase Ended")
log("ETL Job Ended")
