# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    time_stamp = now.strftime(time_stamp_format)
    with open("code_log.txt", "a") as f:
        f.write(time_stamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    try:
        page = requests.get(url, timeout=10)
        page.raise_for_status()
        data = BeautifulSoup(page.text, 'html.parser')
        df = pd.DataFrame(columns=table_attribs)

        # Finding table by heading instead of index
        heading = data.find('span', {'id': 'By_market_capitalization'})
        if not heading:
            raise ValueError("Could not find 'By market capitalization' table")

        table = heading.find_next('table')
        if not table:
            raise ValueError("No table found after heading")

        rows = table.find_all('tr')

        for row in rows[1:11]:  # keeping the limit to top 10
            cols = row.find_all('td')
            if len(cols) >= 3:  # Ensuring we have enough columns
                name = cols[1].get_text(strip=True)
                mc_usd = cols[2].get_text(strip=True).replace(",", "")

                try:
                    mc_usd = float(mc_usd)
                    df = pd.concat([
                        df,
                        pd.DataFrame([{"Name": name, "MC_USD_Billion": mc_usd}])
                    ], ignore_index=True)
                except ValueError:
                    continue

        log_progress('Data extraction complete. Initiating Transformation process')
        return df

    except Exception as e:
        log_progress(f"Error during extraction: {str(e)}")
        return pd.DataFrame(columns=table_attribs)

def transform(df, exchange_rate_file):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    try:
        exchange_rates = pd.read_csv(exchange_rate_file)
        exchange_dict = exchange_rates.set_index('Currency')['Rate'].to_dict()

        df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_dict['GBP'], 2)
        df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_dict['EUR'], 2)
        df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_dict['INR'], 2)

        log_progress('Data transformation complete. Initiating Loading process')
        return df

    except Exception as e:
        log_progress(f"Error during transformation: {str(e)}")
        return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    try:
        df.to_csv(output_path, index=False)
        log_progress('Data saved to CSV file')
    except Exception as e:
        log_progress(f"Error saving to CSV: {str(e)}")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    try:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
        log_progress('Data loaded to Database as table. Running the query')
    except Exception as e:
        log_progress(f"Error loading to database: {str(e)}")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    try:
        print("Executing Query:")
        print(query_statement)
        query_output = pd.read_sql(query_statement, sql_connection)
        print("Query Result:")
        print(query_output)
        log_progress(f"Executed query: {query_statement}")
    except Exception as e:
        log_progress(f"Error executing query: {str(e)}")

if __name__ == "__main__":
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = ["Name", "MC_USD_Billion"]
    db_name = 'Banks.db'
    table_name = 'Largest_banks'
    csv_path = 'Largest_banks_data.csv'
    exchange_rate_file = 'exchange_rate.csv'

    # Start fresh log
    with open("code_log.txt", "w") as f:
        f.write("ETL Process Log\n")

    print("🔄 Starting ETL Process...\n")
    log_progress('Preliminaries complete. Initiating ETL process')

    # Extract
    df = extract(url, table_attribs)

    if not df.empty:
        # Transform
        df = transform(df, exchange_rate_file)

        # Load to CSV
        load_to_csv(df, csv_path)

        # Load to DB & Run Queries
        try:
            with sqlite3.connect(db_name) as conn:
                load_to_db(df, conn, table_name)

                print("\n Top 5 Banks by Market Capitalization:")
                run_query(f"SELECT * FROM {table_name} LIMIT 5", conn)

                print("\n Average Market Capitalization (USD Billion):")
                run_query(f"SELECT AVG(MC_USD_Billion) as Avg_MC_USD FROM {table_name}", conn)

                print("\n Top 3 Banks by Market Capitalization:")
                run_query(f"SELECT Name FROM {table_name} ORDER BY MC_USD_Billion DESC LIMIT 3", conn)

        except Exception as e:
            log_progress(f"Database connection error: {str(e)}")
            print(f" Database Error: {str(e)}")
    else:
        log_progress('ETL process failed - no data extracted')
        print(" Data extraction failed. No data to process.")

    log_progress('ETL process completed')
    print("\n ETL Process Completed Successfully!")
