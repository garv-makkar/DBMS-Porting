import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, lit, array_contains
import sys
import atexit
import os
import signal
import pandas as pd
import shutil
from time import time
from Spark_Queries import query1, query2, query3, query4, query5, query6
from utils import cleanup, sigterm_handler, save_to_csv

spark = None
temp_dir = "./temp"
out = "./query_outputs_partioning"

def create_spark_session():
    global spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("UniversityDBQueries") \
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/university_db") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.local.dir", temp_dir) \
        .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner") \
        .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id") \
        .config("spark.mongodb.input.partitionerOptions.numberOfPartitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

def time_function(func, *args, **kwargs):
    start_time = time()
    result = func(*args, **kwargs)
    end_time = time()
    return result, end_time - start_time

def setup_spark_session():
    global spark
    spark = create_spark_session()
    return spark

def run_queries():
    queries = [
        (query1, os.path.join(out,"query1_result")),
        (query2, os.path.join(out,"query2_result")),
        (query3, os.path.join(out,"query3_result")),
        (query4, os.path.join(out,"query4_result")),
        (query5, os.path.join(out,"query5_result")),
        (query6, os.path.join(out,"query6_result"))
    ]

    execution_times = []
    for query_func, filename in queries:
        result_df, runtime = time_function(query_func, spark)
        execution_times.append({'filename': filename, 'run_time': runtime})
        save_to_csv(result_df, filename)

    return execution_times

def main():
    spark = setup_spark_session()

    os.makedirs(out, exist_ok=True)

    # Warm-up the Spark session
    warm_up_query = spark.sql("SELECT 1")
    warm_up_query.collect()

    execution_times = run_queries()

    # Create a DataFrame to store the execution times
    execution_times_df = pd.DataFrame(execution_times)

    # Save the execution times to a CSV file
    execution_times_df.to_csv(os.path.join(out,'query_execution_times.csv'), index=False)

    cleanup(temp_dir)

if __name__ == "__main__":
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)
    main()