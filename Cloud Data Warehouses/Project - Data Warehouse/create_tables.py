import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

'''
    Function to drop tables. This function uses the variable 'drop_table_queries' defined in 'sql_queries.py' file.
    Parameters:
        - cur: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
'''

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

'''
    Function to create tables. This function uses the variable 'create_table_queries' defined in 'sql_queries.py' file.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()