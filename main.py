import pandas as pd
import mysql.connector as mysqlconnect
import requests
import csv


def writeFile(filename, query_results):
    """Writes the query results into a csv file with given name. Creates one if it does not exist."""

    with open(filename, 'a', newline='') as f_handle:
        # Writing the query results in csv file
        writer = csv.writer(f_handle)
        headers = ['SYMBOL', 'NAME OF COMPANY', 'SERIES',
                   'DATE OF LISTING', 'ISIN NUMBER', 'TIMESTAMP', 'GAINS']
        writer.writerow(headers)

        for row in query_results:
            writer.writerow(row)


def monthToNum(monthName):
    """Returns the month number for three letter abbreviations of months"""

    data = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
            'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
    return data[monthName]


def getFormattedDates(column):
    """Returns the dates given in dd-monthname-yyyy format to yyyy-mm-dd format.

    The function takes in input in the form of an array-like data structure
    consisting of dates in dd-monthname-yyyy format. The monthname is a three-letter
    abbreviation of month name. The function takes these dates and converts them into
    yyyy-mm-dd format which concurs with MySQL format for dates.

    Parameters:
        column: array-like structure consisting of dates in dd-monthname-yyyy format

    Returns:
        array: consists of all dates converted to yyyy-mm-dd format"""

    newdates = []
    for i in range(column.size):
        try:
            date = column[i]
            monthNum = monthToNum(date[3:6])
            newdate = date[7:11] + '-' + monthNum + '-' + date[:2]
            newdates.append(newdate)
        except:
            continue

    return newdates


def createTable(file1, file2):
    """Creates a MySQL Table using given two datasets.

    This function takes in two datasets and merges them into a single MySQL table.
    The joining column in ISIN Number. The function creates a new database and a new table 
    with given names. Deletes the previous versions. 

    Parameters :
        file1 : dataset with required columns
        file2 : dataset with required columns
    """

    # Preprocessing the datasets for insertion into MySQL database

    file1[' DATE OF LISTING'] = getFormattedDates(file1[' DATE OF LISTING'])
    file2['TIMESTAMP'] = getFormattedDates(file2['TIMESTAMP'])

    file2.rename(columns={'ISIN': ' ISIN NUMBER'}, inplace=True)
    file2.drop('SERIES', axis=1, inplace=True)
    file2.drop('SYMBOL', axis=1, inplace=True)

    file = file1.merge(file2, on=' ISIN NUMBER')

    # Connecting with mysql database on local machine

    conn = mysqlconnect.connect(
        host='localhost', user='root', password='password')  # Enter password of root user here

    cursor = conn.cursor()

    # Performing SQL Queries to create a Relational Database Table

    cursor.execute("DROP DATABASE IF EXISTS equity")

    cursor.execute("CREATE DATABASE equity")

    cursor.execute("USE equity")

    cursor.execute("DROP TABLE IF EXISTS equity_table")

    cursor.execute('''CREATE TABLE equity_table(SYMBOL VARCHAR(255),
                        COMPANY_NAME VARCHAR(255), SERIES CHAR(2), DATE_OF_LISTING DATE,
                        PAID_UP_VALUE INT(5), MARKET_LOT INT(5),
                        ISIN_NUMBER CHAR(12), FACE_VALUE INT(5), OPEN FLOAT(8, 3),
                        HIGH FLOAT(8, 3), LOW FLOAT(8, 3), CLOSE FLOAT(8, 3), LAST FLOAT(8, 3), PREVCLOSE FLOAT(8, 3),
                        TOTTRDQTY FLOAT(30, 5),TOTTRDVAL FLOAT(30, 5) , TIMESTAMP DATE, TOTALTRADES INT(10))
                    ''')

    for row in file.itertuples():
        sql = '''INSERT INTO equity_table(SYMBOL, COMPANY_NAME, SERIES, DATE_OF_LISTING,
                        PAID_UP_VALUE, MARKET_LOT, ISIN_NUMBER, FACE_VALUE, OPEN, HIGH, LOW,
                        CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(sql, tuple(row[1:19]))

    conn.commit()

    cursor.close()


def query1():
    "Performs a given query"

    connector = mysqlconnect.connect(
        host='localhost', user='root', password='password')  # Enter password of root user here

    cursor = connector.cursor()

    cursor.execute('''USE equity''')

    # Executing Query 1 and storing the results in a csv file query1results.csv

    query = '''SELECT SYMBOL, COMPANY_NAME, SERIES, DATE_OF_LISTING, ISIN_NUMBER, TIMESTAMP, (CLOSE-OPEN)/OPEN AS GAINS
                    FROM equity_table ORDER BY GAINS DESC LIMIT 25'''
    cursor.execute(query)

    query_results = cursor.fetchall()

    cursor.close()

    writeFile('query1results.csv', query_results)


def query2():
    "Performs a given query"

    connector = mysqlconnect.connect(
        host='localhost', user='root', password='password')  # Enter password of root user here

    cursor = connector.cursor()

    cursor.execute('''USE equity''')

    # Executing Query 1 and storing the results in a csv file query1results.csv

    query = '''SELECT SYMBOL, COMPANY_NAME, SERIES, DATE_OF_LISTING, ISIN_NUMBER, TIMESTAMP, (CLOSE-OPEN)/OPEN AS GAINS
                    FROM equity_table_series ORDER BY GAINS DESC LIMIT 25'''
    cursor.execute(query)

    query_results = cursor.fetchall()

    writeFile("query2results.csv", query_results)

    cursor.close()


def query3():
    "Performs a given query"

    connector = mysqlconnect.connect(
        host='localhost', user='root', password='password')  # Enter password of root user here

    cursor = connector.cursor()

    # Performing SQL Queries to update the 'equity_table' values

    cursor.execute("USE equity")

    cursor.execute("ALTER TABLE equity_table_series ADD NEWCLOSE FLOAT(8, 3)")

    cursor.execute('''UPDATE equity_table_series
                SET NEWCLOSE=(SELECT CLOSE FROM equity_table
                WHERE equity_table_series.ISIN_NUMBER=equity_table.ISIN_NUMBER)''')

    query3 = '''SELECT SYMBOL, COMPANY_NAME, SERIES, DATE_OF_LISTING, ISIN_NUMBER, TIMESTAMP, (NEWCLOSE-OPEN)/OPEN AS GAINS
                    FROM equity_table_series
                    ORDER BY GAINS DESC LIMIT 25'''

    cursor.execute(query3)

    query_results = cursor.fetchall()

    writeFile('query3results.csv', query_results)

    connector.commit()

    cursor.close()


def modifyTable(file1, file2):
    """Creates a MySQL Table using given two datasets.

    This function takes in two datasets and merges them into a single MySQL table.
    The joining column in ISIN Number. The function creates a new table with given name.
    Deletes the previous versions. 

    Parameters :
        file1 : dataset with required columns
        file2 : dataset with required columns
    """

    file1[' DATE OF LISTING'] = getFormattedDates(file1[' DATE OF LISTING'])
    file2['TIMESTAMP'] = getFormattedDates(file2['TIMESTAMP'])

    file2.rename(columns={'ISIN': ' ISIN NUMBER'}, inplace=True)
    file2.drop('SERIES', axis=1, inplace=True)
    file2.drop('SYMBOL', axis=1, inplace=True)

    file = file1.merge(file2, on=' ISIN NUMBER')

    # Connecting with mysql database on local machine

    conn = mysqlconnect.connect(
        host='localhost', user='root', password='password')  # Enter password of root user here

    cursor = conn.cursor()

    # Performing SQL Queries to update the 'equity_table' values

    cursor.execute("USE equity")

    cursor.execute("DROP TABLE IF EXISTS equity_table_series")

    cursor.execute('''CREATE TABLE equity_table_series(SYMBOL VARCHAR(255),
                        COMPANY_NAME VARCHAR(255), SERIES CHAR(2), DATE_OF_LISTING DATE,
                        PAID_UP_VALUE INT(5), MARKET_LOT INT(5),
                        ISIN_NUMBER CHAR(12), FACE_VALUE INT(5), OPEN FLOAT(8, 3),
                        HIGH FLOAT(8, 3), LOW FLOAT(8, 3), CLOSE FLOAT(8, 3), LAST FLOAT(8, 3), PREVCLOSE FLOAT(8, 3),
                        TOTTRDQTY FLOAT(30, 5),TOTTRDVAL FLOAT(30, 5) , TIMESTAMP DATE, TOTALTRADES INT(10))
                    ''')

    for row in file.itertuples():
        sql = '''INSERT INTO equity_table_series(SYMBOL, COMPANY_NAME, SERIES, DATE_OF_LISTING,
                        PAID_UP_VALUE, MARKET_LOT, ISIN_NUMBER, FACE_VALUE, OPEN, HIGH, LOW,
                        CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(sql, tuple(row[1:19]))

    conn.commit()
    cursor.close()


# __main__

endpoint1 = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
endpoint2 = "https://archives.nseindia.com/content/historical/EQUITIES/2022/DEC/cm09DEC2022bhav.csv.zip"

# Downloading and opening the datasets using pandas library
dataset1 = pd.read_csv(endpoint1)
file1 = pd.DataFrame(dataset1)

dataset2 = pd.read_csv(endpoint2)
file2 = pd.DataFrame(dataset2)

createTable(file1.copy(), file2)

# Calling query1 function to perform query no. 1
query1()

# Source Code for query no.2
# This uses a method similar to web-scraping to access each dataset for the last 30 days
# We progressively decrease the date and day and procure the bhavcopy csv file
# excluding Saturdays and Sundays

month = "DEC"
date = 9
dayCount = 30
day = 5

urlFront = "https://archives.nseindia.com/content/historical/EQUITIES/2022/"
urlEnd = "2022bhav.csv.zip"

while (dayCount):
    date_str = str(date)

    if (date < 10):
        date_str = "0" + date_str

    endpoint = urlFront + month + "/" + "cm" + date_str + month + urlEnd

    if (day > 0 and day < 6):
        dataset = pd.read_csv(endpoint)
        data = pd.DataFrame(dataset)

        # Modifying the table in our database to perform further queries
        modifyTable(file1.copy(), data)

        # Calling query1 function to perform query no. 2
        query2()

    date -= 1
    if (date == 0):
        date = 30
        month = "NOV"

    dayCount -= 1

    day -= 1
    if (day < 0):
        day = 6


# Calling query3 function to perform query no. 3
query3()
