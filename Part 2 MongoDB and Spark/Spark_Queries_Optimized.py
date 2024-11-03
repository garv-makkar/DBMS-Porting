import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, lit, array_contains, count, explode, broadcast
import sys
import atexit
import os
import signal
import pandas as pd
import shutil

def query1_optimized(spark):
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.students") \
        .option("collection", "students") \
        .load()
    result = students_df.filter(array_contains(col("enrolled_courses"), 15)) \
        .select("student_name", "student_email") \
        .cache()  # Cache the result if it's used multiple times
    return result

from pyspark.sql.functions import broadcast

def query2_optimized(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.courses") \
        .option("collection", "courses") \
        .load()
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.students") \
        .option("collection", "students") \
        .load()
    
    filtered_courses = courses_df.filter(col("instructor_id") == 2).cache()
    
    result = filtered_courses \
        .join(broadcast(students_df), array_contains(students_df.enrolled_courses, filtered_courses._id)) \
        .groupBy(filtered_courses._id, "course_code") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "num_students") \
        .agg(avg("num_students").alias("average_students"))
    
    return result

def query3_optimized(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.courses") \
        .option("collection", "courses") \
        .load() \
        .filter(col("department_id") == 1) \
        .select("course_code", "department_id", "is_core_course") \
        .dropDuplicates(["course_code"]) \
        .cache()  # Cache if used multiple times
    return courses_df

from pyspark.sql.functions import broadcast

def query4_optimized(spark):
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.students") \
        .option("collection", "students") \
        .load()
    departments_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.departments") \
        .option("collection", "departments") \
        .load()
    
    result = students_df.groupBy("department_id") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "total_students") \
        .join(broadcast(departments_df), col("department_id") == departments_df._id) \
        .select(departments_df.department_name, "total_students")
    
    return result

from pyspark.sql.functions import broadcast

def query5_optimized(spark):
    instructors_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.instructors") \
        .option("collection", "instructors") \
        .load()
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.courses") \
        .option("collection", "courses") \
        .load() \
        .filter((col("department_id") == 1) & (col("is_core_course") == True)) \
        .cache()
    
    cse_core_courses_count = courses_df.count()
    
    result = instructors_df.join(broadcast(courses_df), instructors_df._id == courses_df.instructor_id) \
        .groupBy(instructors_df._id, "instructor_name") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "taught_core_courses") \
        .filter(col("taught_core_courses") == cse_core_courses_count) \
        .select("instructor_name")
    
    return result

def query6_optimized(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.courses") \
        .option("collection", "courses") \
        .load()
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db.students") \
        .option("collection", "students") \
        .load()
    
    result = students_df.select(explode("enrolled_courses").alias("course_id")) \
        .groupBy("course_id") \
        .agg(count("*").alias("num_enrollments")) \
        .join(courses_df, col("course_id") == courses_df._id) \
        .select("course_code", "num_enrollments") \
        .orderBy(col("num_enrollments").desc()) \
        .limit(10)
    
    return result