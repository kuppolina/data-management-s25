import psycopg2 
from queries_postgresql import run
from config import load_config 

def connect_postgresql(config):
    try: 
        conn = psycopg2.connect(**config)
        print('Connected to the PostgerSQL server')
        run(conn)
        return conn # do i need it here, that is the question 
    except (psycopg2.DatabaseError, Exception) as error: 
        print(error)
        return None 
    

if __name__ == '__main__': 
    config = load_config()
    connect_postgresql(config)  