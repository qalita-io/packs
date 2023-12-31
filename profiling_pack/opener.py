# opener.py
import os
import glob
import pandas as pd
from sqlalchemy import create_engine

# Mapping of default ports to database types
DEFAULT_PORTS = {
    '5432': 'postgresql',
    '3306': 'mysql',
    '1433': 'mssql+pymssql',
}

# Function to load data file
def load_data_file(file_path):
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path, low_memory=False, memory_map=True, on_bad_lines="skip")
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, engine='openpyxl')

# Function to create database connection
def create_db_connection(config):
    user = config['username']
    password = config['password']
    host = config['host']
    port = config['port']
    type = config['type']
    db = config['database']
    
    if type:
        db_type = type
    else:
        # Deduce the database type from the port
        db_type = DEFAULT_PORTS.get(port, 'unknown')
        if db_type == 'unknown':
            raise ValueError(f"Unsupported or unknown database port: {port}")

    engine = create_engine(f'{db_type}://{user}:{password}@{host}:{port}/{db}')
    return engine

# Function to load data from database
def load_data_from_db(engine):
    with engine.connect() as connection:
        # Check liveness
        try:
            connection.execute('SELECT 1')
        except Exception as e:
            raise ConnectionError(f"Database connection failed: {e}")

        # Scan tables
        tables = engine.table_names()
        if not tables:
            raise ValueError("No tables found in the database.")

        # Load each table into a DataFrame and return them
        dataframes = {}
        for table in tables:
            dataframes[table] = pd.read_sql_table(table, engine)
        
        return dataframes
    
# Function to load data based on the configuration
def load_data(config):
    source_type = config['type']

    if source_type == "file":
        path = config["config"]["path"]

        if os.path.isfile(path):
            if path.endswith('.csv') or path.endswith('.xlsx'):
                return load_data_file(path)
            else:
                raise ValueError("Unsupported file type. Only CSV and XLSX are supported.")
        elif os.path.isdir(path):
            data_files = glob.glob(os.path.join(path, "*.csv")) + glob.glob(os.path.join(path, "*.xlsx"))
            if not data_files:
                raise FileNotFoundError("No CSV or XLSX files found in the provided path.")
            first_data_file = data_files[0]
            return load_data_file(first_data_file)
        else:
            raise FileNotFoundError(f"The path {path} is neither a file nor a directory.")
    
    elif source_type == "database":
        db_config = config['config']
        engine = create_db_connection(db_config)
        return load_data_from_db(engine)

    else:
        raise ValueError("Unsupported source type. Only 'file' and 'database' are supported.")
