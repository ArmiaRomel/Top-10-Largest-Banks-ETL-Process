import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
table_name = 'Largest_banks'
db_name = 'Banks.db'

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) !=0 :
            if col[1].find('a') is not None:
                df = pd.concat([df, pd.DataFrame([{'Name': col[1].get_text().strip(), 'MC_USD_Billion': float(col[2].contents[0])}], index=[0])], ignore_index=True)

    return df

def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path)
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['Rate'][1], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['Rate'][0], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['Rate'][2], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(pd.read_sql(query_statement, sql_connection))


log_progress('ETL Process Started')

log_progress('Extraction Process Started')
extracted_data = extract(url, table_attribs)
log_progress('Extraction Process Ended')

log_progress('Transformation Process Started')
transformed_data = transform(extracted_data, csv_path)
log_progress('Transformation Process Ended')

log_progress('Loading To CSV Process Started')
load_to_csv(transformed_data, output_path)
log_progress('Loading To CSV Process Ended')

log_progress('Database connection Started')
conn = sqlite3.connect(db_name)

log_progress('Loading To Database Process Started')
load_to_db(transformed_data, conn, table_name)
log_progress('Loading To Database Process Ended')

log_progress('Select All From Database Process Started')
run_query('SELECT * FROM Largest_banks', conn)
log_progress('Select All From Database Process Ended')

log_progress('Calculate Average GBP Process Started')
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
log_progress('Calculate Average GBP Process Ended')

log_progress('Select 5 Names Process Started')
run_query('SELECT Name from Largest_banks LIMIT 5', conn)
log_progress('Select 5 Names Process Ended')

conn.close()
log_progress('Database connection Ended')

log_progress('ETL Process Ended')
