"""
This script will perform the ETL job. First it loads the data into the DWH schema.
Then it will execute some queries for testing and analytics.

The script import resources from the sql_queries script
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, row_table_queries, analytic_table_queries
from timeit import default_timer
import pandas.io.sql as sqlio
import warnings


def load_staging_tables(cur, conn):
    """Load the data from the s3 bucket into the staging tables"""

    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Load initiation: ")
    var = 1
    for query in copy_table_queries:
        print("Executing COPY query " + str(var) + "...")
        var += 1
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert the data from the staging tables to the start schema tables"""

    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Insert initiation: ")
    var = 1
    for query in insert_table_queries:
        print("Executing INSERT query " + str(var) + "...")
        cur.execute(query)
        conn.commit()


def row_tables(conn):
    """Executes a query to check database population with row units for table"""

    warnings.filterwarnings('ignore')
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Checking number of rows per table: ")
    for query in row_table_queries:
        data = sqlio.read_sql_query(query, conn)
        print(query)
        # cur.execute(query)
        # result = cur.fetchone()[0]
        print(data)
        #print("rows: " + str(result))
        conn.commit()


def analytic_tables(conn):
    """Executes analytic queries to analyze components and performance"""

    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Test queries for analytics: ")
    warnings.filterwarnings('ignore')
    var = 1
    for query in analytic_table_queries:
        print('\n')
        print("+++ Query " + str(var) + " +++")
        data = sqlio.read_sql_query(query, conn)
        print(query)
        print("+++ Query " + str(var) + " result: +++")
        print('\n')
        print(data)
        conn.commit()
        var += 1


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connecting to the cluster database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # executing the data load
    start = default_timer()
    load_staging_tables(cur, conn)
    duration = default_timer() - start
    print("Load completed")
    print("Load time duration: " + str(duration))

    # executing data insertion
    start = default_timer()
    insert_tables(cur, conn)
    duration = default_timer() - start
    print("Insert completed")
    print("Insert time duration: " + str(duration))

    # executing row queries
    row_tables(conn)
    print('\n')
    print("Number of rows completed")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print('\n')

    # executing analytic queries
    start = default_timer()
    analytic_tables(conn)
    duration = default_timer() - start
    print('\n')
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print('\n')
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Analytic completed")
    print("Analytic time duration: " + str(duration))
    print('\n')
    print("ETL job done!!")


    conn.close()


if __name__ == "__main__":
    main()
