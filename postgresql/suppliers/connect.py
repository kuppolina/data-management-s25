import psycopg2 
from config import load_config 

def connect(config):
    try: 
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgerSQL server')
            # here add the functions for performance evaluation; 
            # list the types of queries for evaluation 
            # prepare the queries 
            # load the db suppliers with data 
            return conn
    except (psycopg2.DatabaseError, Exception) as error: 
        print(error)
        

if __name__ == '__main__': 
    config = load_config()
    connect(config)  