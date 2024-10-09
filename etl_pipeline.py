import logging
from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, inspect
from sqlalchemy import String, Integer, Float, DateTime, Boolean,text
import pandas as pd
import os
from dotenv import load_dotenv
import time
from utilities import retry,create_table,infer_schema,normalize_mongo_data,detect_foreign_keys,extract_data_and_infer_schema



logging.basicConfig(
    filename='etl_pipeline_dynamic.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

if not MONGO_URI or not MONGO_DB_NAME:
    logging.critical("MongoDB connection details are missing in the .env file.")
    raise Exception("MongoDB connection details are missing.")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# PostgreSQL connection using environment variables
POSTGRESQL_URI = os.getenv('POSTGRESQL_URI')

if not POSTGRESQL_URI:
    logging.critical("PostgreSQL connection details are missing in the .env file.")
    raise Exception("PostgreSQL connection details are missing.")

engine = create_engine(POSTGRESQL_URI)


collections = ["clients", "suppliers", "sonar_runs", "sonar_results"]

# Initialize metadata for SQLAlchemy
metadata = MetaData()

# Dictionary to hold all schemas for foreign key detection
all_schemas = {}

# Step 1: Extract data from MongoDB and infer schema dynamically
dfs = {}
schemas = {}



@retry(max_attempts=3, delay=5)
def etl_pipeline():
    for collection_name in collections:
        df, schema = extract_data_and_infer_schema(collection_name)
        dfs[collection_name] = df
        schemas[collection_name] = schema
        all_schemas[collection_name] = schema  # Store schema for foreign key detection

# Detect foreign keys dynamically based on field names
    foreign_keys = {}
    for collection_name, schema in schemas.items():
        foreign_keys[collection_name] = detect_foreign_keys(schema, all_schemas)

#Create tables dynamically in PostgreSQL
    tables = {}
    for collection_name, schema in schemas.items():
        table = create_table(collection_name, schema, foreign_keys[collection_name], metadata)
        tables[collection_name] = table

# Handle Many-to-Many Relationships for 'supplier_ids' in 'sonar_runs'
# We remove 'supplier_ids' from sonar_runs and create a separate association table
    association_tables = {}
    if 'sonar_runs' in tables and 'suppliers' in tables:
        association_table_name = 'sonar_runs_suppliers'
        association_tables[association_table_name] = {
        'source': 'sonar_runs',
        'target': 'suppliers',
        'source_field': '_id',
        'target_field': '_id'
        }
        assoc_schema = {
        'sonar_run_id': String,
        'supplier_id': String
        }
        assoc_foreign_keys = {
        'sonar_run_id': 'sonar_runs',
        'supplier_id': 'suppliers'
        }
    #assoc_table = create_table(association_table_name, assoc_schema, assoc_foreign_keys, metadata)
        assoc_table = Table(
        association_table_name, metadata,
        Column('sonar_run_id', String, ForeignKey('sonar_runs._id')),
        Column('supplier_id', String, ForeignKey('suppliers._id'))
        )
        tables[association_table_name] = assoc_table

# Create all tables in the database
    metadata.create_all(engine)

    logging.info("Tables created dynamically with foreign keys.")

# Load data into PostgreSQL
    try:
        inspector = inspect(engine)
        for collection_name, df in dfs.items():
            # Remove 'supplier_ids' from sonar_runs as it will be handled in the association table
            if collection_name == 'sonar_runs' and 'supplier_ids' in df.columns:
                df = df.drop(columns=['supplier_ids'])
            
            for col in ['_id', 'client_id', 'supplier_id', 'part_id', 'sonar_run_id']:
                if col in df.columns:
                    df[col] = df[col].astype(str)

            if inspector.has_table(collection_name):
                df.to_sql(collection_name, engine, if_exists='append', index=False)
            else:
                df.to_sql(collection_name, engine, if_exists='replace', index=False)

        # Handle association table for 'supplier_ids' in 'sonar_runs'
        if 'supplier_ids' in dfs['sonar_runs'].columns:
            association_records = []
            for idx, row in dfs['sonar_runs'].iterrows():
                for supplier_oid in row['supplier_ids']:
                    # Extract just the supplier ObjectId value
                    if isinstance(supplier_oid, dict) and '$oid' in supplier_oid:
                        supplier_oid = supplier_oid['$oid']
                    association_records.append({
                        'sonar_run_id': row['_id'], 
                        'supplier_id': supplier_oid  # Store only the actual ObjectId value, not the dict
                    })
            association_df = pd.DataFrame(association_records)
            # Convert all fields to strings to avoid type errors
            association_df['sonar_run_id'] = association_df['sonar_run_id'].astype(str)
            association_df['supplier_id'] = association_df['supplier_id'].astype(str)
            
            
            association_df.to_sql(association_table_name, engine, if_exists='replace', index=False)
        
        with engine.begin() as conn:

            # Adding indexes to each table
            indexes = {
            'clients': ['_id'],
            'suppliers': ['_id', 'country'],  
            'sonar_runs': ['_id', 'client_id'],
            'sonar_results': ['_id', 'sonar_run_id', 'part_id'],
            'sonar_runs_suppliers': ['sonar_run_id', 'supplier_id']
            }
        
            for table_name, columns in indexes.items():
                for column in columns:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{column} ON {table_name} ({column});"))
                    logging.info(f"Created index idx_{table_name}_{column} on table {table_name}.")

            sql_files = ['alter_tables.sql', 'create_views.sql']
            for sql_file in sql_files:
                    file_path = os.path.join('sql_scripts', sql_file)
                    with open(file_path, 'r') as f:
                        sql_statements = f.read()
                        conn.execute(text(sql_statements))
                        logging.info(f"Successfully executed SQL script: {sql_file}")



            logging.info("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error loading data into PostgreSQL: {e}")
        raise



if __name__ == "__main__":
    try:
        etl_pipeline()
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.critical(f"ETL pipeline failed: {e}")