import psycopg2 
from .config import load_config 

def connect_postgresql(config):
    try: 
        conn = psycopg2.connect(**config)
        print('Connected to the PostgerSQL server')
        return conn
    except (psycopg2.DatabaseError, Exception) as error: 
        print(error)
        return None 
    

if __name__ == '__main__': 
    config = load_config()
    connect_postgresql(config)  