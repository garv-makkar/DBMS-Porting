import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, lit, array_contains
import sys
import atexit
import os
import signal
import pandas as pd
import shutil

def query1(spark):
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.students") \
        .option("collection", "students") \
        .load()
    result = students_df.filter(array_contains(col("enrolled_courses"), 15)).select("student_name", "student_email")
    return result

def query2(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.courses") \
        .option("collection", "courses") \
        .load()
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.students") \
        .option("collection", "students") \
        .load()
    
    result = courses_df.filter(col("instructor_id") == 2) \
        .join(students_df, array_contains(students_df.enrolled_courses, courses_df._id)) \
        .groupBy(courses_df._id, "course_code") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "num_students") \
        .agg(avg("num_students").alias("average_students"))
    
    return result

def query3(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.courses") \
        .option("collection", "courses") \
        .load()
    result = courses_df.filter(col("department_id") == 1) \
        .select("course_code", "department_id", "is_core_course") \
        .dropDuplicates(["course_code"])
    return result

def query4(spark):
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.students") \
        .option("collection", "students") \
        .load()
    departments_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.departments") \
        .option("collection", "departments") \
        .load()
    
    result = students_df.groupBy("department_id") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "total_students") \
        .join(departments_df, students_df.department_id == departments_df._id) \
        .select(departments_df.department_name, "total_students")
    
    return result

def query5(spark):
    instructors_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.instructors") \
        .option("collection", "instructors") \
        .load()
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.courses") \
        .option("collection", "courses") \
        .load()
    
    # Rename department_id in courses_df to avoid ambiguity
    courses_df = courses_df.withColumnRenamed("department_id", "course_department_id")
    
    cse_core_courses = courses_df.filter((col("course_department_id") == 1) & (col("is_core_course") == True)).select("_id")
    
    result = instructors_df.join(courses_df, instructors_df._id == courses_df.instructor_id) \
        .filter((col("course_department_id") == 1) & (col("is_core_course") == True)) \
        .groupBy(instructors_df._id, "instructor_name") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "taught_core_courses") \
        .join(cse_core_courses.agg({"*": "count"}).withColumnRenamed("count(1)", "total_core_courses"), lit(1) == lit(1)) \
        .filter(col("taught_core_courses") == col("total_core_courses")) \
        .select("instructor_name")
    
    return result

def query6(spark):
    courses_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.courses") \
        .option("collection", "courses") \
        .load()
    students_df = spark.read.format("mongodb") \
        .option("uri", "mongodb://localhost:27017/university_db_indexing.students") \
        .option("collection", "students") \
        .load()
    
    result = courses_df.join(students_df, array_contains(students_df.enrolled_courses, courses_df._id)) \
        .groupBy(courses_df._id, "course_code") \
        .agg({"*": "count"}) \
        .withColumnRenamed("count(1)", "num_enrollments") \
        .orderBy(col("num_enrollments").desc()) \
        .limit(10)
    
    return result