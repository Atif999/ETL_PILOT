
from pymongo import MongoClient
import json
from bson import ObjectId 
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

# MongoDB connection setup
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]  

# Function to transform JSON data for MongoDB compatibility
def transform_json_data(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                if '$oid' in value:
                    data[key] = ObjectId(value['$oid']) 
                elif '$date' in value:
                    data[key] = datetime.fromisoformat(value['$date'].replace('Z', ''))  
                else:
                    transform_json_data(value)  
            elif isinstance(value, list):
                for item in value:
                    transform_json_data(item)  
    return data

# Function to load JSON files, transform data, and insert into MongoDB
def insert_data_into_mongo(collection_name, json_file):
    with open(json_file) as f:
        data = json.load(f)
        transformed_data = []
        for item in data:
            transformed_item = transform_json_data(item)  # Transform the JSON data
            transformed_data.append(transformed_item)
        print(f"Transformed data for {collection_name}: {transformed_data}")  # Debug output
        try:
            db[collection_name].insert_many(transformed_data)  # Insert into MongoDB collection
            print(f"Data successfully inserted into '{collection_name}' collection")
        except Exception as e:
            print(f"Error inserting data into '{collection_name}': {e}")

# Insert data into MongoDB collections
insert_data_into_mongo('clients', 'clients.json')
insert_data_into_mongo('suppliers', 'suppliers.json')
insert_data_into_mongo('sonar_runs', 'sonar_runs.json')
insert_data_into_mongo('sonar_results', 'sonar_results.json')
