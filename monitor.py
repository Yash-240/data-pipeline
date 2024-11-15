import pandas as pd
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import sqlite3

# Define the folder path to monitor
FOLDER_TO_MONITOR = # local path 
TARGET_FILENAME = # local path example.csv
QUARANTINE_FOLDER = # local path 
DB_PATH = # local path  example.db

class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Check if the created file is the target file
        if event.is_directory:
            return  # Ignore directories
        if os.path.basename(event.src_path) == TARGET_FILENAME:
            print(f"Detected file: {TARGET_FILENAME}")
            # Call the next process here
            validate_data(event.src_path)

def validate_data(file_path):
    print(f"Starting data Validation for: {file_path}")
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Validation 1: Check for null values in required columns
        if df[['sensor_id', 'timestamp', 'pressure', 'temperature', 'humidity']].isnull().any().any():
            print("Validation Failed: Null values found in required columns.")
            quarantine_file(file_path, "Null values in required columns.")
            return False

        # Validation 2: Check temperature range
        if not df['temperature'].between(-1000, 1000).all():
            print("Validation Failed: Temperature out of range.")
            quarantine_file(file_path, "Temperature out of range.")
            return False

        # Validation 3: Check humidity for negative values
        if (df['humidity'] < 0).any():
            print("Validation Failed: Negative humidity values found.")
            quarantine_file(file_path, "Negative humidity values.")
            return False

        # Validation 4: Check data type of sensor_id
        if not pd.api.types.is_integer_dtype(df['sensor_id']):
            print("Validation Failed: sensor_id is not an integer.")
            quarantine_file(file_path, "sensor_id is not an integer.")
            return False

        print("Validation Passed.")
        # Call the next process if validation passes
        start_data_transformation(file_path)
        return True

    except Exception as e:
        print(f"Error in validation: {e}")
        quarantine_file(file_path, str(e))
        return False

def quarantine_file(file_path, reason):
    """Move the file to quarantine folder with an error log."""
    if not os.path.exists(QUARANTINE_FOLDER):
        os.makedirs(QUARANTINE_FOLDER)
    base_name = os.path.basename(file_path)
    quarantine_path = os.path.join(QUARANTINE_FOLDER, base_name)
    os.rename(file_path, quarantine_path)
    print(f"File moved to quarantine due to: {reason}")


def start_data_transformation(file_path):
    print(f"Starting data transformation for: {file_path}")
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # 1. Split the 'timestamp' column into 'date' and 'time' columns
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        df['time'] = pd.to_datetime(df['timestamp']).dt.time

        # 2. Round the 'lat' and 'lon' columns to 3 decimal places
        df['lat'] = df['lat'].round(3)
        df['lon'] = df['lon'].round(3)

        # 3. Save the transformed data back to the same path and target filename
        df.to_csv(file_path, index=False)
        print(f"Data transformation completed and saved to: {file_path}")

        save_raw_data_to_db(file_path)
        calculate_and_store_aggregated_metrics()


    except Exception as e:
        print(f"Error in data transformation: {e}")

def save_raw_data_to_db(file_path):
    print(f"Saving raw data to database for: {file_path}")
    try:
        # Read the CSV file after transformation (this assumes it's already transformed)
        df = pd.read_csv(file_path)

        # Connect to SQLite database (if it doesn't exist, it will be created)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            sensor_id INTEGER,
            location INTEGER,
            lat REAL,
            lon REAL,
            timestamp TEXT,
            pressure REAL,
            temperature REAL,
            humidity REAL,
            date TEXT,
            time TEXT
        );
        """
        cursor.execute(create_table_query)

        # 2. Insert data into the table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO sensor_data (sensor_id, location, lat, lon, timestamp, pressure, temperature, humidity, date, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(insert_query, (
                row['sensor_id'], row['location'], row['lat'], row['lon'],
                row['timestamp'], row['pressure'], row['temperature'], 
                row['humidity'], row['date'], row['time']
            ))

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

        print(f"Raw data saved to database: {DB_PATH}")

        
    except Exception as e:
        print(f"Error saving data to database: {e}")


def calculate_and_store_aggregated_metrics():
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Fetch data from sensor_data table (ensure you have the required columns)
        query = """
        SELECT sensor_id, 
               DATE(timestamp) AS date, 
               temperature, 
               'data_2017_07' AS filename
        FROM sensor_data
        """
        df = pd.read_sql_query(query, conn)

        # Group by sensor_id and date to calculate aggregated metrics
        aggregated_df = df.groupby(['sensor_id', 'date']).agg(
            min_temperature=('temperature', 'min'),
            max_temperature=('temperature', 'max'),
            avg_temperature=('temperature', 'mean'),
            stddev_temperature=('temperature', 'std')
        ).reset_index()

        # Add filename column (assuming filename is the same for each group)
        aggregated_df['filename'] = df['filename'].iloc[0]  # You can modify this if needed

        # Create the aggregated_metrics table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS aggregated_metrics (
            sensor_id INTEGER,
            date DATE,
            min_temperature REAL,
            max_temperature REAL,
            avg_temperature REAL,
            stddev_temperature REAL,
            filename TEXT
        )
        ''')

        # Insert aggregated metrics into the table
        for _, row in aggregated_df.iterrows():
            cursor.execute('''
            INSERT INTO aggregated_metrics (sensor_id, date, min_temperature, max_temperature, avg_temperature, stddev_temperature, filename)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (row['sensor_id'], row['date'], row['min_temperature'], row['max_temperature'],
                  row['avg_temperature'], row['stddev_temperature'], row['filename']))

        # Commit changes and close the connection
        conn.commit()
        print("Aggregated metrics stored successfully.")
        conn.close()

    except Exception as e:
        print(f"Error during aggregation and insertion: {e}")

def monitor_folder():
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_MONITOR, recursive=False)
    observer.start()
    print(f"Monitoring folder: {FOLDER_TO_MONITOR}")

    try:
        while True:
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    monitor_folder()