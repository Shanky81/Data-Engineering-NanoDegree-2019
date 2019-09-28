import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
'''
    Function to load stage tables. This function uses the variable 'copy_table_queries' defined in 'sql_queries.py' file.
    Parameters:
        - cur: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
'''

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
'''
    Function to insert into tables. This function uses the variable 'insert_table_queries' defined in 'sql_queries.py' file.
    Parameters:
        - cur: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
'''

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()