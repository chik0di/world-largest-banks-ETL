# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime  

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def todays_rates(url_rates):
    response = requests.get(url_rates)
    soup = BeautifulSoup(response.text, 'html.parser')

    rates = soup.find_all('tbody')
    todays_rate = []
    for rate in rates:
        sections = rate.find_all('tr')

        for section in sections:
            exchange = section.text.strip().split('\n')[:-1]
            todays_rate.append(exchange)
            
    xR = pd.DataFrame(todays_rate, columns=['currency', 'rate'])
    xR.to_csv('exchange_rate.csv')

    return xR

def extract(url, table_attribs):
    #  This function aims to extract the required information from the website and save it to a data frame

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table_attribs = ['Name', 'MarketCap_USDollar_Billion']
    dfc = pd.DataFrame(columns=table_attribs)

    table = soup.find('tbody')
    rows = table.find_all('tr')
    data = []
    for row in rows[1:]: 
        item = [text.strip() for text in row.text.split('\n') if text.strip()]
        item = item[1:]
        if len(item) >= 2:  # Ensure there are at least two columns
            data.append({'Name': item[0], 'MarketCap_USDollar_Billion': item[1]})
            
    df = pd.concat([dfc, pd.DataFrame(data)], ignore_index=True)
    df['MarketCap_USDollar_Billion'] = df['MarketCap_USDollar_Billion'].str.replace(',', '').astype(float)


    return df

def transform(df, xR_path):
    #  Transforms Market Cap values into multiple currencies using rates from the exchange rate CSV file.

    exchange = pd.read_csv(xR_path)
    exchange_rate = exchange.set_index('Currency').to_dict()['Rate']

    for currency, rate in exchange_rate.items():
        df[f'MarketCap_{currency}_Billion'] = np.round(df['MarketCap_USDollar_Billion'] * rate, 2)

    return df

def load_to_csv(df):
    # This function saves the final data frame as a CSV file 

    df.to_csv('Largest_banks.csv')

def load_to_db(df, sql_connection, table_name):
    # This function saves the final data frame to a database table with the provided table name 

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

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"

url_rates =  "https://www.x-rates.com/table/?from=USD&amount=1"

table_attribs = ["Country", "MC_USD_Billion"]

table_name = 'Largest_banks'

xR_path = './exchange_rate.csv'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, xR_path)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name  FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Queries run successfully')

log_progress('Process Complete.')

sql_connection.close()