import pandas as pd
import numpy as np
import  requests
from bs4 import BeautifulSoup
import  sqlite3
import datetime as datetime



# URL of the Wikipedia page containing the data

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'


# Logging the progress of the ETL process
def log_progress(message):
    now = datetime.datetime.now()
    with open('code_log.txt', 'a') as log_file:
        log_file.write(f"{now} - {message}\n")


log_progress("ETL Process has started")

# Extracting the data
def extract(url):
    response = requests.get(url)
    if(response.status_code == 200):
        print('Request is successful')
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find_all('table')[0]
        data = pd.read_html(str(table))[0]

        extracted_data = data.iloc[:,-2:].rename(columns = {'Bank name':'Name','Market cap (US$ billion)':'MC_USD_Billion'})

        print(extracted_data)
        log_progress('Data extraction completed')

        return extracted_data

    else:
        log_progress('Request failed')


# Transforming the data
def transform(data) :

    exchange_rate = pd.read_csv('exchange_rate.csv')

    print(exchange_rate)

    data['MC_GBP_Billion'] = round(data['MC_USD_Billion'] * exchange_rate.loc[exchange_rate['Currency'] == 'GBP', 'Rate'].values[0],2)
    data['MC_EUR_Billion'] = round(data['MC_USD_Billion'] * exchange_rate.loc[exchange_rate['Currency'] == 'EUR', 'Rate'].values[0],2)
    data['MC_INR_Billion'] = round(data['MC_USD_Billion'] * exchange_rate.loc[exchange_rate['Currency'] == 'INR', 'Rate'].values[0],2)

    print(data)

    log_progress('Data transformation completed')

    print(data['MC_EUR_Billion'][4])

    return data

# Loading the data to a CSV file
def load_to_csv(input_data,target_file):
    target_file ='./World_Bank_Data.csv'
    input_data.to_csv(target_file, index = False)

    log_progress('Data has been saved to the target file')

# Loading the data to a SQLite database
def load_to_db(input_data):

    conn = sqlite3.connect('Bank.db')
    input_data.to_sql('Largest_banks', conn, if_exists = 'replace', index = False)
    conn.close()

    log_progress('Data has been saved to the database')



log_progress("ETL Process has ended")

# Running a query on the database
def run_query(query):

    conn = sqlite3.connect('Bank.db')
    data = pd.read_sql(query, conn)
    print(data)
    conn.close()
    log_progress('Query has been run successfully')

    return data



# Running the ETL process

if __name__ == '__main__':

    extracted_data = extract(url)

    transformed_data = transform(extracted_data)

    load_to_csv(transformed_data, './World_Bank_Data.csv')

    load_to_db(transformed_data)

    query1 = 'SELECT * FROM Largest_banks'
    query2 ='SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    query3 ='SELECT Name from Largest_banks LIMIT 5'

    run_query(query1)
    run_query(query2)
    run_query(query3)




