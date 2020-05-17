import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Read the configuration file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Get the params from the configuration file
DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

def load_staging_tables(cur, conn):
    """
    Loading data from S3 to the staging tables in our Redshift cluster
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from the staging tables to the dimensional tables
    """
    for query in insert_table_queries:
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
    
    print('[INFO] Loading staging tables to the Redshift cluster...')
    load_staging_tables(cur, conn)
    
    print('[INFO] Inserting data from the staging tables to the dimensional tables...')
    insert_tables(cur, conn)
    
    print('[INFO] End of the ETL')
    
    conn.close()

if __name__ == "__main__":
    main()