import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import sys

# Read the configuration file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Get the params from the configuration file
DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

def drop_tables(cur, conn):
    """
    Drop the tables if they exist just to make sure that the Redshift cluster is clean
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create the staging and dimensional tables in the Redshift cluster
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    try:
        print('[INFO] Connecting to the Redshift cluster...')
        # Connect to the Redshift cluster
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
        cur = conn.cursor()
        print('[INFO] [OK] Connected to the Redshift cluster!')
    except Exception as e:
        print('[ERROR] Error on connecting to the Redshift cluster')
        print(e)

    print('[INFO] Droping the tables if they exist in the Redshift cluster...')
    drop_tables(cur, conn)
    
    print('[INFO] Creating the tables in the Redshift cluster...')
    create_tables(cur, conn)

    print('[INFO] End of the creation of the tables')
    
    conn.close()


if __name__ == "__main__":
    main()