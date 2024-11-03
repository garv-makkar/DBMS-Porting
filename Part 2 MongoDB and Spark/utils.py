import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, lit, array_contains
import sys
import atexit
import os
import signal
import pandas as pd
import shutil
import time

spark = None 

def force_delete_temp_directory(temp_dir):
    try:
        print(f"Deleting temp directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        print(f"Temp directory {temp_dir} deleted.")
    except Exception as e:
        print(f"Error while deleting temp directory: {e}. Retrying...")

        retries = 2
        wait_time = 1  
        for attempt in range(retries):
            time.sleep(wait_time) 
            try:
                print(f"Retrying deletion of {temp_dir} (Attempt {attempt + 1})")
                shutil.rmtree(temp_dir)
                print(f"Temp directory {temp_dir} deleted.")
                break  
            except Exception as e:
                print(f"Retry {attempt + 1} failed: {e}")
                
                print("Checking for locked files...")
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        if is_file_locked(file_path):
                            print(f"File is locked: {file_path}")

        else:
            print(f"Failed to delete {temp_dir} after {retries} attempts.")

def is_file_locked(filepath):
    try:
        with open(filepath, 'a'):
            return False
    except IOError:
        return True


def cleanup(temp_dir):
    global spark
    if spark:
        try:
            print("Stopping Spark session...")
            spark.stop()
            print("Spark session stopped.")
        except Exception as e:
            print(f"Error during Spark cleanup: {e}")
    
    if os.path.exists(temp_dir):
        force_delete_temp_directory(temp_dir)
    
    print("Exiting script.")
    os._exit(0)


def sigterm_handler(signum, frame):
    print("Received termination signal. Cleaning up...")
    cleanup()


def save_to_csv(df, filename):
    df_pandas = df.toPandas()
    output_csv_path = f"./{filename}.csv"
    df_pandas.to_csv(output_csv_path, index=False)
    print(f"Data saved to {output_csv_path}")