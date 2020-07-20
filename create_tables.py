import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Discription:    
        Function that executes the drop table queries.
    
    Arguments:
        cur and conn: cursor object and connector
    Return: 
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Discription:    
        Function that executes the create table queries.
    
    Arguments:
        cur and conn: cursor object and connector
    Return: 
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main(Endpoint):
    """
    Discription:    
        Function that executes the drop table queries.
    
    Arguments:
        Endpoint: Endpoint object which locates the etnrypoint to the database
    Return: 
        None
    """
    #Phrase config files
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    #Set up connections
    conn_string="postgresql://{}:{}@{}:{}/{}".format(config.get("CLUSTER","DB_USER"), config.get("CLUSTER","DB_PASSWORD"), Endpoint, config.get("CLUSTER","DB_PORT"),       config.get("CLUSTER","DB_NAME"))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    #set_dist(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("tables created")

if __name__ == "__main__":
    main()