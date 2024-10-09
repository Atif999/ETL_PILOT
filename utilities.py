import logging
from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, inspect
from sqlalchemy import String, Integer, Float, DateTime, Boolean,text
import pandas as pd
import os
from dotenv import load_dotenv
import time
# import smtplib
#from email.mime.text import MIMEText


# Function for re-running the pipeline if fail initally
def retry(max_attempts=3, delay=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt} failed: {e}")
                    if attempt == max_attempts:
                        logging.critical("Max attempts reached. Pipeline failed.")
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


def normalize_mongo_data(data):
    normalized_data = []
    for item in data:
        normalized_item = {}
        for key, value in item.items():
            if isinstance(value, dict):
                if "$oid" in value:
                    normalized_item[key] = str(value["$oid"])  # Convert ObjectId to string
                elif "$date" in value:
                    normalized_item[key] = pd.to_datetime(value["$date"])  # Convert Date to datetime
                else:
                    normalized_item[key] = normalize_mongo_data([value])[0]  # Recursively normalize nested dicts
            elif isinstance(value, list):
                normalized_item[key] = [normalize_mongo_data([v])[0] if isinstance(v, dict) else v for v in value]  # Normalize each item in the list
            else:
                normalized_item[key] = value
        normalized_data.append(normalized_item)
    return normalized_data

# Function to infer schema from MongoDB documents
def infer_schema(collection_data):
    schema = {}
    for document in collection_data:
        for key, value in document.items():
            if key == "_id":  # Skip the _id field for schema inference
                continue
            if key not in schema:
                if isinstance(value, str):
                    schema[key] = String
                elif isinstance(value, int):
                    schema[key] = Integer
                elif isinstance(value, float):
                    schema[key] = Float
                elif isinstance(value, pd.Timestamp):
                    schema[key] = DateTime
                elif isinstance(value, bool):
                    schema[key] = Boolean
                else:
                    schema[key] = String  # Default to string for complex or unknown types
    return schema

# Function to automatically detect foreign key relationships based on field names
def detect_foreign_keys(schema, all_schemas):
    foreign_keys = {}
    for field in schema:
        if field.endswith("_id") and field != "_id":
            ref_table = field[:-3] + "s"  # e.g., 'client_id' refers to 'clients'
            if ref_table in all_schemas:
                foreign_keys[field] = ref_table
    return foreign_keys

# Function to create tables dynamically based on schema
def create_table(name, schema, foreign_keys, metadata):
    columns = [Column('_id', String, primary_key=True)]  # Only add _id column once
    for field, field_type in schema.items():
        if field != "_id":  # Skip _id field
            if field in foreign_keys:
                columns.append(Column(field, field_type, ForeignKey(f"{foreign_keys[field]}._id")))
            else:
                columns.append(Column(field, field_type))
    table = Table(name, metadata, *columns)
    return table

# Extract MongoDB data and infer schema
def extract_data_and_infer_schema(collection_name,db):
    collection = db[collection_name]
    data = list(collection.find({}))
    normalized_data = normalize_mongo_data(data)
    schema = infer_schema(normalized_data)
    return pd.DataFrame(normalized_data), schema

# Function to send email alerts in case of critical pipeline failure
# def send_alert_email(error_message,sender_email,recevier_email,server_name,username,password):
#     sender = sender_email
#     receivers = [recevier_email]
#     msg = MIMEText(f"ETL Pipeline Failure: {error_message}")
#     msg['Subject'] = 'ETL Pipeline Alert'
#     msg['From'] = sender
#     msg['To'] = ', '.join(receivers)

#     with smtplib.SMTP(server_name) as server:
#         server.login(username, password)
#         server.sendmail(sender, receivers, msg.as_string())