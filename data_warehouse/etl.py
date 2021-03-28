import configparser
import argparse
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for name, query in copy_table_queries.items():
        print(name)
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for name, query in insert_table_queries.items():
        print(name)
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    parser = argparse.ArgumentParser(description='Sparkfy ETL')
    parser.add_argument('--load-stg', action='store_true', help='sum the integers (default: find the max)')
    parser.add_argument('--insert-dw', action='store_true', help='sum the integers (default: find the max)')

    args = parser.parse_args()
    print(args)

    conn = None
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
            .format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        if args.load_stg:
            load_staging_tables(cur, conn)
        elif args.insert_dw:
            insert_tables(cur, conn)
        else:
            load_staging_tables(cur, conn)
            insert_tables(cur, conn)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()