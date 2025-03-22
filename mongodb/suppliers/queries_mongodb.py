import time
import inspect
import pymongo

def process_query_append_new_values_to_array_special_features(mydb):
    try:        
        mycol = mydb["films"]
        
        result = mycol.update_one({"_id": 5}, {"$push": {"special_features": "Exclusive Interview"}}) 
        
        print("Executed process_query_append_new_values_to_array_special_features")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

def process_query_with_is_contained(mydb):
    try:
        mycol = mydb["films"]
        mycol.find({"special_features": "Trailers"}) 
        
        print("Executed process_query_with_contains")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

def process_query_with_unnest(mydb):
    try:
        mycol = mydb["films"]
        mycol.aggregate([{"$unwind":"$special_features"}])
        print("Executed process_query_with_unsset")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

def run(client):
    mydb = client["suppliers"]

    funcs = [func for name, func in globals().items() 
             if callable(func) and name.startswith("process_query_")]

    with open("execution_times_mongodb.txt", "w") as file:
        for func in funcs:
            try:
                start_time = time.process_time()
                func(mydb)  
                end_time = time.process_time()
                
                execution_time = end_time - start_time
                file.write(f"{func.__name__}, {execution_time:.6f}, MongoDB\n")

            except Exception as e:
                print(f"Failed to execute {func.__name__}: {e}")

if __name__ == "__main__":
    run()
