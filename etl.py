import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Loading staging table complete")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Insert table complete")


def main(Endpoint):
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn_string="postgresql://{}:{}@{}:{}/{}".format(config.get("CLUSTER","DB_USER"), config.get("CLUSTER","DB_PASSWORD"), Endpoint, config.get("CLUSTER","DB_PORT"),       config.get("CLUSTER","DB_NAME"))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()