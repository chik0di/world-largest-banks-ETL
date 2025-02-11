# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime  


def log_progress(message, log_file="./code_log.txt", overwrite=False):
    """Logs a message to a file, overwriting only if overwrite=True (typically at script start)."""
    
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # Get current timestamp
    timestamp = now.strftime(timestamp_format)

    mode = "w" if overwrite else "a"  # "w" overwrites, "a" appends
    with open(log_file, mode) as f:
        f.write(timestamp + ' : ' + message + '\n')

def todays_rates():

    url_rates = "https://www.x-rates.com/table/?from=USD&amount=1"
    response = requests.get(url_rates)
    soup = BeautifulSoup(response.text, 'html.parser')

    tab = soup.find('table', class_="tablesorter ratesTable")
    rates = tab.find_all('tbody')
    todays_rate = []

    for rate in rates:
        sections = rate.find_all('tr')
        for section in sections:
            exchange = section.text.strip().split('\n')[:-1]
            todays_rate.append(exchange)
            
    xR = pd.DataFrame(todays_rate, columns=['currency', 'rate'])
    xR.to_csv('exchange_rate.csv')

    return xR

def extract():
    #  This function aims to extract the required information from the website and save it to a data frame

    url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find_all('table')[2]
    rows = table.find_all('tbody')
    banks = []

    for row in rows: 
        items = row.find_all('tr')
        for item in items[2:]:
            bank = item.text.strip().split('\n')
            bank_name = bank[0]
            market_cap = bank[4]
            banks.append([bank_name, market_cap])

    df = pd.DataFrame(banks, columns=['Bank Name', 'MarketCap_USDollar_Billion'])
    empty_rows = df[df['MarketCap_USDollar_Billion'] == ''].index
    df.drop(empty_rows, inplace=True)
    df['MarketCap_USDollar_Billion'] = df['MarketCap_USDollar_Billion'].astype(float)

    return df

def transform(df, xR_path):
    #  Transforms Market Cap values into multiple currencies using rates from the exchange rate CSV file.

    exchange = pd.read_csv(xR_path)
    exchange_rate = exchange.set_index('currency').to_dict()['rate']

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


table_name = 'Largest_banks'
xR_path = './exchange_rate.csv'


log_progress('Preliminaries complete. Initiating ETL process', overwrite=True)

todays_rates()
log_progress('Successfully extracted exchange rates as at the time this message was logged and stored in a CSV file')

df = extract()
log_progress('Data extraction complete. Initiating Transformation process...')

df = transform(df, xR_path)
log_progress('Data transformation complete. Initiating loading process...')

load_to_csv(df)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the queries')

# query_statement = f"SELECT * from {table_name}"
# run_query(query_statement, sql_connection)

# query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
# run_query(query_statement, sql_connection)

# query_statement = f"SELECT Name  FROM {table_name} LIMIT 5"
# run_query(query_statement, sql_connection)

# log_progress('Queries run successfully')

log_progress('Process Complete.')

sql_connection.close()