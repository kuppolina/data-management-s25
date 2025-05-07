import time
import psycopg2
import inspect

def query_append_to_array(conn):
    set_up_append_value(conn)
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT film_id, title,  ARRAY_APPEND(special_features, 'Exclusive Interview') FROM film where film_id = 7;")
        
        print("Executed process_query_append_new_values_to_array_special_features")  
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def query_is_contained(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("Select film_id, title, special_features from film where special_features @> ARRAY['Trailers'];")
        
        print("Executed process_query_with_contains")  
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def query_unnest(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("Select film_id, title, release_year, unnest(special_features) as “features” from film;")
        
        print("Executed process_query_with_unsset")  
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
# Query with joins 
def query_joins(conn): 
    try:
        with conn.cursor() as cur:
            cur.execute("With count_films as (select c.name , count (f.film_id) as amount from film f, category c, film_category fc where f.film_id = fc.film_id and c.category_id = fc.category_id group by c.name) select * from count_films where amount >= 60; ")
        
        print("Executed process_query_with_joins")  
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 

# Query with operation on dictionaries (key:value pairs) 
def process_query_with_dict(conn): 
    try: 
        with conn.cursor() as cur:
            cur.execute("")
        
        print("Executed process_query_with_dict")  
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 

# Function to find and execute all process_query functions
def run(conn):
    funcs = [func for name, func in globals().items() 
             if callable(func) and name.startswith("query_")]

    with open("execution_times_postgresql.txt", "w") as file:
        for func in funcs:
            try:
                execution_time = get_average_time(func, conn)
                file.write(f"{func.__name__}, {execution_time:.6f}, PostgreSQL\n")

            except Exception as e:
                print(f"Failed to execute {func.__name__}: {e}")
                
def get_average_time(func, conn):
    average_times = []
    for i in range(0, 10):
        start_time = time.process_time()
        func(conn)  
        end_time = time.process_time()
        execution_time = end_time - start_time
        average_times.append(execution_time)
    return sum(average_times) / len(average_times); 

def set_up_append_value(conn):
    try:
        with conn.cursor() as cur:
            # change the query 
            cur.execute("SELECT film_id, title,  ARRAY_REMOVE(special_features, 'Exclusive Interview') FROM film where film_id = 7;")
        
        print("Executed clean up append new value")    
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == "__main__":
    run()
