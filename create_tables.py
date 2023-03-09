"""
This scrip has two main functions:
 * Create the Redshift cluster in AWS
 * Create the staging and the star schema tables for our Data Warehouse

It imports the resources from two different scripts: cluster_connection and sql_queries
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from cluster_connection import cluster_connect


def drop_tables(cur, conn):
    """Drops tables if already exist before a new creation process"""

    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Dropping tables if necessary...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates the tables"""

    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("Creating tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # This command will initiate the creation of the Redshift cluster
    cluster_connect()

    # connecting to the cluster Data Warehouse
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)
    cur = conn.cursor()

    # Dropping the tables if already exist
    drop_tables(cur, conn)

    # Creating the tables
    create_tables(cur, conn)
    print("tables created!!")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
