# Necessary Libraries
import os
import json
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime

# PostgreSQL connection 
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="data_repo",
    user="postgres",
    password="Netid#7100"  # My Password
)
cursor = conn.cursor()

base_dir = r"C:\Users\Sunny\pulse\data"

# Creating schema if not exists
cursor.execute("CREATE SCHEMA IF NOT EXISTS phonepe;")
conn.commit()

# Creating tables
create_tables_query = """
-- Aggregated Transaction Table
CREATE TABLE IF NOT EXISTS phonepe.aggregated_transaction (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    transaction_type VARCHAR(100),
    transaction_count BIGINT,
    transaction_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated User Table
CREATE TABLE IF NOT EXISTS phonepe.aggregated_user (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    brand VARCHAR(100),
    user_count BIGINT,
    percentage DECIMAL(10, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated Insurance Table
CREATE TABLE IF NOT EXISTS phonepe.aggregated_insurance (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    insurance_type VARCHAR(100),
    insurance_count BIGINT,
    insurance_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Map Transaction Table
CREATE TABLE IF NOT EXISTS phonepe.map_transaction (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    district VARCHAR(100),
    transaction_count BIGINT,
    transaction_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Map User Table
CREATE TABLE IF NOT EXISTS phonepe.map_user (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    district VARCHAR(100),
    registered_users BIGINT,
    app_opens BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Map Insurance Table
CREATE TABLE IF NOT EXISTS phonepe.map_insurance (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    district VARCHAR(100),
    insurance_count BIGINT,
    insurance_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Top Transaction Table
CREATE TABLE IF NOT EXISTS phonepe.top_transaction (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    entity_type VARCHAR(50),
    entity_name VARCHAR(100),
    transaction_count BIGINT,
    transaction_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Top User Table
CREATE TABLE IF NOT EXISTS phonepe.top_user (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    entity_type VARCHAR(50),
    entity_name VARCHAR(100),
    registered_users BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Top Insurance Table
CREATE TABLE IF NOT EXISTS phonepe.top_insurance (
    id SERIAL PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    entity_type VARCHAR(50),
    entity_name VARCHAR(100),
    insurance_count BIGINT,
    insurance_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Executing table creation
for statement in create_tables_query.split(';'):
    if statement.strip():
        cursor.execute(statement)
conn.commit()

print("Tables created successfully!")

def insert_aggregated_transaction():
    """Insert aggregated transaction data"""
    path = os.path.join(base_dir, "aggregated", "transaction", "country", "india")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    # Processing country level data
    for year in os.listdir(path):
        year_path = os.path.join(path, year)
        if os.path.isdir(year_path) and year.isdigit():
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter = int(quarter_file.split('.')[0])
                    file_path = os.path.join(year_path, quarter_file)
                    
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                        
                        if 'data' in data and 'transactionData' in data['data']:
                            for transaction in data['data']['transactionData']:
                                data_list.append((
                                    'India',
                                    int(year),
                                    quarter,
                                    transaction['name'],
                                    transaction['paymentInstruments'][0]['count'],
                                    float(transaction['paymentInstruments'][0]['amount'])
                                ))
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
    
    # Processing state level data
    state_path = os.path.join(path, "state")
    if os.path.exists(state_path):
        for state in os.listdir(state_path):
            state_dir = os.path.join(state_path, state)
            if os.path.isdir(state_dir):
                for year in os.listdir(state_dir):
                    year_path = os.path.join(state_dir, year)
                    if os.path.isdir(year_path) and year.isdigit():
                        for quarter_file in os.listdir(year_path):
                            if quarter_file.endswith('.json'):
                                quarter = int(quarter_file.split('.')[0])
                                file_path = os.path.join(year_path, quarter_file)
                                
                                try:
                                    with open(file_path, 'r') as f:
                                        data = json.load(f)
                                    
                                    if 'data' in data and 'transactionData' in data['data']:
                                        for transaction in data['data']['transactionData']:
                                            data_list.append((
                                                state.replace('-', ' ').title(),
                                                int(year),
                                                quarter,
                                                transaction['name'],
                                                transaction['paymentInstruments'][0]['count'],
                                                float(transaction['paymentInstruments'][0]['amount'])
                                            ))
                                except Exception as e:
                                    print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.aggregated_transaction 
            (state, year, quarter, transaction_type, transaction_count, transaction_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into aggregated_transaction")
    else:
        print("No data found for aggregated_transaction")

def insert_aggregated_user():
    """Insert aggregated user data"""
    path = os.path.join(base_dir, "aggregated", "user", "country", "india")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    # Processing country level data first
    for year in os.listdir(path):
        year_path = os.path.join(path, year)
        if os.path.isdir(year_path) and year.isdigit():
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter = int(quarter_file.split('.')[0])
                    file_path = os.path.join(year_path, quarter_file)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Checking if data exists and has the expected structure
                        if 'data' in data:
                            if 'usersByDevice' in data['data'] and data['data']['usersByDevice'] is not None:
                                for device in data['data']['usersByDevice']:
                                    if device is not None:  # Additional check
                                        data_list.append((
                                            'India',
                                            int(year),
                                            quarter,
                                            device.get('brand', 'Unknown'),
                                            device.get('count', 0),
                                            float(device.get('percentage', 0))
                                        ))
                            else:
                                print(f"No usersByDevice data in {file_path}")
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error in file {file_path}: {str(e)}")
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
    
    # Processing state level data
    state_path = os.path.join(path, "state")
    if os.path.exists(state_path):
        for state in os.listdir(state_path):
            state_dir = os.path.join(state_path, state)
            if os.path.isdir(state_dir):
                for year in os.listdir(state_dir):
                    year_path = os.path.join(state_dir, year)
                    if os.path.isdir(year_path) and year.isdigit():
                        for quarter_file in os.listdir(year_path):
                            if quarter_file.endswith('.json'):
                                quarter = int(quarter_file.split('.')[0])
                                file_path = os.path.join(year_path, quarter_file)
                                
                                try:
                                    with open(file_path, 'r', encoding='utf-8') as f:
                                        data = json.load(f)
                                    
                                    # Checking if data exists and has the expected structure
                                    if 'data' in data:
                                        if 'usersByDevice' in data['data'] and data['data']['usersByDevice'] is not None:
                                            for device in data['data']['usersByDevice']:
                                                if device is not None:  # Additional check
                                                    data_list.append((
                                                        state.replace('-', ' ').title(),
                                                        int(year),
                                                        quarter,
                                                        device.get('brand', 'Unknown'),
                                                        device.get('count', 0),
                                                        float(device.get('percentage', 0))
                                                    ))
                                        else:
                                            print(f"No usersByDevice data in {file_path}")
                                except json.JSONDecodeError as e:
                                    print(f"JSON decode error in file {file_path}: {str(e)}")
                                except Exception as e:
                                    print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.aggregated_user 
            (state, year, quarter, brand, user_count, percentage)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into aggregated_user")
    else:
        print("No data found for aggregated_user")

def insert_map_transaction():
    """Insert map transaction data"""
    path = os.path.join(base_dir, "map", "transaction", "hover", "country", "india", "state")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    for state in os.listdir(path):
        state_dir = os.path.join(path, state)
        if os.path.isdir(state_dir):
            for year in os.listdir(state_dir):
                year_path = os.path.join(state_dir, year)
                if os.path.isdir(year_path) and year.isdigit():
                    for quarter_file in os.listdir(year_path):
                        if quarter_file.endswith('.json'):
                            quarter = int(quarter_file.split('.')[0])
                            file_path = os.path.join(year_path, quarter_file)
                            
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                
                                if 'data' in data and 'hoverDataList' in data['data']:
                                    for district_data in data['data']['hoverDataList']:
                                        data_list.append((
                                            state.replace('-', ' ').title(),
                                            int(year),
                                            quarter,
                                            district_data['name'].title(),
                                            district_data['metric'][0]['count'],
                                            float(district_data['metric'][0]['amount'])
                                        ))
                            except Exception as e:
                                print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.map_transaction 
            (state, year, quarter, district, transaction_count, transaction_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into map_transaction")
    else:
        print("No data found for map_transaction")

def insert_map_user():
    """Insert map user data"""
    path = os.path.join(base_dir, "map", "user", "hover", "country", "india", "state")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    for state in os.listdir(path):
        state_dir = os.path.join(path, state)
        if os.path.isdir(state_dir):
            for year in os.listdir(state_dir):
                year_path = os.path.join(state_dir, year)
                if os.path.isdir(year_path) and year.isdigit():
                    for quarter_file in os.listdir(year_path):
                        if quarter_file.endswith('.json'):
                            quarter = int(quarter_file.split('.')[0])
                            file_path = os.path.join(year_path, quarter_file)
                            
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                
                                if 'data' in data and 'hoverData' in data['data']:
                                    for district_name, district_data in data['data']['hoverData'].items():
                                        data_list.append((
                                            state.replace('-', ' ').title(),
                                            int(year),
                                            quarter,
                                            district_name.title(),
                                            district_data.get('registeredUsers', 0),
                                            district_data.get('appOpens', 0)
                                        ))
                            except Exception as e:
                                print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.map_user 
            (state, year, quarter, district, registered_users, app_opens)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into map_user")
    else:
        print("No data found for map_user")

def insert_top_transaction():
    """Insert top transaction data"""
    path = os.path.join(base_dir, "top", "transaction", "country", "india")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    # Processing country level data
    for year in os.listdir(path):
        year_path = os.path.join(path, year)
        if os.path.isdir(year_path) and year.isdigit():
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter = int(quarter_file.split('.')[0])
                    file_path = os.path.join(year_path, quarter_file)
                    
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                        
                        if 'data' in data:
                            # Process states
                            if 'states' in data['data']:
                                for item in data['data']['states']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'state',
                                        item['entityName'].title(),
                                        item['metric']['count'],
                                        float(item['metric']['amount'])
                                    ))
                            
                            # Processing districts
                            if 'districts' in data['data']:
                                for item in data['data']['districts']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'district',
                                        item['entityName'].title(),
                                        item['metric']['count'],
                                        float(item['metric']['amount'])
                                    ))
                            
                            # Processing pincodes
                            if 'pincodes' in data['data']:
                                for item in data['data']['pincodes']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'pincode',
                                        str(item['entityName']),
                                        item['metric']['count'],
                                        float(item['metric']['amount'])
                                    ))
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.top_transaction 
            (state, year, quarter, entity_type, entity_name, transaction_count, transaction_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into top_transaction")
    else:
        print("No data found for top_transaction")

def insert_top_user():
    """Insert top user data"""
    path = os.path.join(base_dir, "top", "user", "country", "india")
    
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return
    
    data_list = []
    
    # Processing country level data
    for year in os.listdir(path):
        year_path = os.path.join(path, year)
        if os.path.isdir(year_path) and year.isdigit():
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter = int(quarter_file.split('.')[0])
                    file_path = os.path.join(year_path, quarter_file)
                    
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                        
                        if 'data' in data:
                            # Process states
                            if 'states' in data['data']:
                                for item in data['data']['states']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'state',
                                        item['name'].title(),
                                        item['registeredUsers']
                                    ))
                            
                            # Processing districts
                            if 'districts' in data['data']:
                                for item in data['data']['districts']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'district',
                                        item['name'].title(),
                                        item['registeredUsers']
                                    ))
                            
                            # Processing pincodes
                            if 'pincodes' in data['data']:
                                for item in data['data']['pincodes']:
                                    data_list.append((
                                        'India',
                                        int(year),
                                        quarter,
                                        'pincode',
                                        str(item['name']),
                                        item['registeredUsers']
                                    ))
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
    
    # Inserting data
    if data_list:
        insert_query = """
            INSERT INTO phonepe.top_user 
            (state, year, quarter, entity_type, entity_name, registered_users)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        execute_batch(cursor, insert_query, data_list, page_size=1000)
        conn.commit()
        print(f"Inserted {len(data_list)} records into top_user")
    else:
        print("No data found for top_user")

# Main execution
if __name__ == "__main__":
    print("Starting ETL process...")
    print(f"Base directory: {base_dir}")
    
    # Debugging function to check JSON structure
    def debug_json_structure():
        """Debug function to check the structure of JSON files"""
        print("\n=== Debugging JSON Structure ===")
        
        # Checking a sample aggregated user file
        sample_path = os.path.join(base_dir, "aggregated", "user", "country", "india", "state")
        if os.path.exists(sample_path):
            states = os.listdir(sample_path)
            if states:
                first_state = states[0]
                state_path = os.path.join(sample_path, first_state)
                years = os.listdir(state_path)
                if years and years[0].isdigit():
                    year_path = os.path.join(state_path, years[0])
                    files = os.listdir(year_path)
                    if files:
                        sample_file = os.path.join(year_path, files[0])
                        print(f"\nChecking file: {sample_file}")
                        try:
                            with open(sample_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            print(f"Keys in JSON: {list(data.keys())}")
                            if 'data' in data:
                                print(f"Keys in 'data': {list(data['data'].keys())}")
                                if 'usersByDevice' in data['data']:
                                    print(f"usersByDevice type: {type(data['data']['usersByDevice'])}")
                                    if data['data']['usersByDevice'] and isinstance(data['data']['usersByDevice'], list):
                                        print(f"First device entry: {data['data']['usersByDevice'][0]}")
                        except Exception as e:
                            print(f"Error reading sample file: {e}")
    
    try:
        # Checking if base directory exists
        if not os.path.exists(base_dir):
            print(f"ERROR: Base directory does not exist: {base_dir}")
            print("Please make sure you have cloned the PhonePe Pulse repository correctly.")
        else:
            # Running debug to understand structure
            debug_json_structure()
            
            print("\nLoading aggregated transaction data...")
            insert_aggregated_transaction()
            
            print("\nLoading aggregated user data...")
            insert_aggregated_user()
            
            print("\nLoading map transaction data...")
            insert_map_transaction()
            
            print("\nLoading map user data...")
            insert_map_user()
            
            print("\nLoading top transaction data...")
            insert_top_transaction()
            
            print("\nLoading top user data...")
            insert_top_user()
            
            print("\nETL process completed successfully!")
            
            # Printing summary
            cursor.execute("SELECT COUNT(*) FROM phonepe.aggregated_transaction")
            agg_trans_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM phonepe.aggregated_user")
            agg_user_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM phonepe.map_transaction")
            map_trans_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM phonepe.map_user")
            map_user_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM phonepe.top_transaction")
            top_trans_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM phonepe.top_user")
            top_user_count = cursor.fetchone()[0]
            
            print("\n=== Data Loading Summary ===")
            print(f"Aggregated Transaction: {agg_trans_count} records")
            print(f"Aggregated User: {agg_user_count} records")
            print(f"Map Transaction: {map_trans_count} records")
            print(f"Map User: {map_user_count} records")
            print(f"Top Transaction: {top_trans_count} records")
            print(f"Top User: {top_user_count} records")
            
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")