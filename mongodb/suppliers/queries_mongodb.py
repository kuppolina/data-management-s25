import time
import inspect
import pymongo

# append new value to the array in films to the column 'special features'
# clean up would be deleting this new value 
def query_append_to_array(mydb):
    set_up_append_value(mydb)
    try:        
        mycol = mydb["films"]
        
        result = mycol.update_one({"_id": 5}, {"$push": {"special_features": "Exclusive Interview"}}) 

        print("Executed query_append_to_array")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

# find query -> just retrieve the information, no clean up needed 
def query_is_contained(mydb):
    try:
        mycol = mydb["films"]
        result = mycol.find({"special_features": "Trailers"}) 
        list(result)
        
        print("Executed process_query_with_contains")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

# process unnest functionality, no clean up needed 
def query_unnest(mydb):
    try:
        mycol = mydb["films"]
        result = mycol.aggregate([{"$unwind":"$special_features"}])
        list(result)
        
        print("Executed process_query_with_unsset")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)
        
# retrievs data with lookuops aka joins     
# Query with lookups  
def query_joins(mydb): 
    try:
        mycol = mydb["film_category"]
        result = mycol.aggregate(
            [
                {
                    "$lookup": {
                        "from": "categories",
                        "localField": "category_id",
                        "foreignField": "category_id",
                        "as": "category_details"
                    }
                },
                {
                    "$unwind": "$category_details"
                },
                {
                    "$lookup": {
                        "from": "films",
                        "localField": "film_id",
                        "foreignField": "film_id",
                        "as": "film_details"
                    }
                },
                {
                    "$unwind": "$film_details"
                }, 
                {
                    "$group": {
                        "_id": "category_details.name", 
                        "amount": {"$sum":1}
                    }
                },
                {
                    "$match": {
                        "amount": {"$gte": 60}
                    }
                }, 
                {
                    "$project": {
                        "_id": 0,
                        "name":"$_id",
                        "amount":1
                    }
                }
            ])  
        
        list(result)
        print("Executed process_query_with_lookup")  
    
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

def run(client):
    mydb = client["suppliers"]

    funcs = [func for name, func in globals().items() 
             if callable(func) and name.startswith("query_")]

    with open("execution_times_mongodb.txt", "w") as file:
        for func in funcs:
            try: 
                execution_time = get_average_time(func, mydb)
                file.write(f"{func.__name__}, {execution_time:.6f}, MongoDB\n")

            except Exception as e:
                print(f"Failed to execute {func.__name__}: {e}")
                
                
def get_average_time(func, mydb):
    average_times = []
    for i in range(0, 10):
        start_time = time.process_time()
        func(mydb)  
        end_time = time.process_time()
        execution_time = end_time - start_time
        average_times.append(execution_time)
    
    return sum(average_times) / len(average_times); 

def set_up_append_value(mydb):
    try:        
        mycol = mydb["films"]
        
        result = mycol.update_one({"_id": 5}, {"$pull": {"special_features": "Exclusive Interview"}}) 
        
        print("Executed clean up append new value")  
        
    except pymongo.errors.OperationFailure as error:
        print("MongoDB Operation Failure:", error)
    except Exception as e:
        print("An unexpected error occurred:", e)

if __name__ == "__main__":
    run()