import pymongo
import certifi
import os
from dotenv import load_dotenv
from load_transaction_data import load
from testing import testing
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def connect_mongodb(): 
    load_dotenv()  # Load .env variables
    
    # Retrieve credentials securely
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        print("Missing MongoDB credentials in .env file.")
        return None

    uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    try: 
        client = MongoClient(uri, 
                        server_api=ServerApi('1'),
                        tlsCAFile=certifi.where()
                        )
        client.admin.command('ping')
        print("You successfully connected to MongoDB")

        # load(client)
        testing(client)
        
    except Exception as e:
        print(e)
        
connect_mongodb()