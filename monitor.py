import pandas as pd
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

# Define the folder path to monitor
FOLDER_TO_MONITOR = "/Users/yashwanth/Documents/bosch_internship/data_pipeline/data"
TARGET_FILENAME = "data_2017_07.csv"
QUARANTINE_FOLDER = "/Users/yashwanth/Documents/bosch_internship/data_pipeline/quarantine"

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

    except Exception as e:
        print(f"Error in data transformation: {e}")

    print("Data transformation completed.")


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


