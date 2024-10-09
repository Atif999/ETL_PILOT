# ETL Pipeline for Data Processing

This project is an ETL (Extract, Transform, Load) pipeline that extracts data from JSON files, transforms it for MongoDB compatibility, loads it into MongoDB, and then processes it into PostgreSQL for analytical queries.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- **Python 3.x** installed on your machine. You can download it from [python.org](https://www.python.org/downloads/).
- **MongoDB**: You should have access to a MongoDB instance. You can use a local installation or a cloud provider like MongoDB Atlas.
- **PostgreSQL**: You should have PostgreSQL installed and running. You can download it from [postgresql.org](https://www.postgresql.org/download/).
- Create a `.env` file to store your environment variables for MongoDB and PostgreSQL connection strings.

## Getting Started

Follow these steps to set up the project on your local machine:

1. **Clone the Repository**

   Open a terminal or command prompt and run the following command:

   ```bash
   git clone https://github.com/Atif999/ETL_PILOT.git
   cd <repository-directory>
   ```

2. **Install Required Libraries**

You can install the required Python libraries using pip. The project have a requirements.txt file that lists all the dependencies. You can install the required libraries using:

`pip install -r requirements.txt`

3. **Configure Environment Variables**

Create a .env file in the root of your project directory and add your MongoDB and PostgreSQL connection strings. The file should look like this:

MONGO_URI='your_mongodb_uri' <br\>
MONGO_DB_NAME='your_database_name' <br\>
POSTGRESQL_URI='your_postgresql_uri'

Replace your_mongodb_uri, your_database_name, and your_postgresql_uri with your actual database credentials

4. **Run the Project**

**Upload Data to MongoDB (if necessary)**

If you do not have the initial data in MongoDB, you can run the data upload script. This script reads the JSON files and inserts the data into the specified MongoDB collections. You can execute it using:

`python data_upload.py`

You can now run the ETL pipeline. In your terminal, execute:

`python etl_pipeline.py`

5. **Error Handling**

If you encounter any issues, check the etl_pipeline_dynamic.log file for detailed logging information about the pipeline execution.

6. **Testing the Application**

Once the ETL pipeline has run successfully, you can test the application and run queries to see the loaded data.

Connecting to PostgreSQL
You can use any PostgreSQL client, such as psql, DBeaver, or pgAdmin, to connect to your PostgreSQL database. Use the following connection details:

Host: localhost (or your PostgreSQL server)
Database Name: your_database_name
User: your_username
Password: your_password

You can do that in two ways:

1. Run your queries directly for analytics
   How many results do we have
   o … per part and shop?
   o … per country?
   o … per customer?
   • How did prices per part develop over time?

or you can

2. Check the views as well that has been made which are faster to retreive the data
