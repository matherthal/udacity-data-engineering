import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from configparser import ConfigParser

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    config = ConfigParser()
    config.read('config.cfg')
    
    # connect to default database
    conn = psycopg2.connect(**dict(config['default_db']))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(**dict(config['default_db']))
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Args:
        cur (cursor): a cursor to sparkfydb
        conn (connection): the connection to sparkfydb
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    
    Args:
        cur (cursor): a cursor to sparkfydb
        conn (connection): the connection to sparkfydb
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Recreate the sparkfydb database and tables
    
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()