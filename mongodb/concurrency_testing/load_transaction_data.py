import csv
from pymongo import MongoClient

def load(client):    
    mydb = client["concurrency_testing"]
    mycol = mydb["transactions"]

    # Load and insert CSV
    with open('/Users/polinakuptsova/Documents/hslu/data-management/homework/mongodb/concurrency_testing/bank.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        data = list(reader)  # Convert to list of dicts
        mycol.insert_many(data)

    print("CSV inserted successfully.")