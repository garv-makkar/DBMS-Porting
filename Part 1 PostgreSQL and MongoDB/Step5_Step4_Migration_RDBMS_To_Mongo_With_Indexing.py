import psycopg2
from pymongo import MongoClient

# Database connection details for PostgreSQL
pg_db_name = ""
pg_user = ""  # Replace with your PostgreSQL username
pg_password = ""  # Replace with your PostgreSQL password
pg_host = ""  # Host of the PostgreSQL server
pg_port = ""  # Default PostgreSQL port

# MongoDB connection details, add yours
mongo_client = MongoClient('')
mongo_db = mongo_client[""]

# PostgreSQL Connection
pg_conn = psycopg2.connect(
    dbname=pg_db_name,
    user=pg_user,
    password=pg_password,
    host=pg_host,
    port=pg_port
)
pg_cursor = pg_conn.cursor()

# Helper: Drop collections in MongoDB to start fresh
def reset_mongo_collections():
    mongo_db.departments.drop()
    mongo_db.students.drop()
    mongo_db.instructors.drop()
    mongo_db.courses.drop()

# Create indexes to optimize query performance
def create_indexes():
    # Indexes for departments
    mongo_db.departments.create_index([("_id", 1)])  # Unique index on department_id

    # Indexes for students
    mongo_db.students.create_index([("_id", 1)])  # Unique index on student_id
    mongo_db.students.create_index([("department_id", 1)])  # Index on department_id for filtering by department

    # Indexes for instructors
    mongo_db.instructors.create_index([("_id", 1)])  # Unique index on instructor_id
    mongo_db.instructors.create_index([("department_id", 1)])  # Index on department_id for filtering

    # Indexes for courses
    mongo_db.courses.create_index([("_id", 1)])  # Unique index on course_id
    mongo_db.courses.create_index([("department_id", 1)])  # Index on department_id for filtering
    mongo_db.courses.create_index([("instructor_id", 1)])  # Index on instructor_id to link courses with instructors

    # Index for enrollments embedded in students
    mongo_db.students.create_index([("enrolled_courses", 1)])  # Index for querying by enrolled courses

    print("Indexes created successfully!")

# Migrate Departments
def migrate_departments():
    pg_cursor.execute("SELECT department_id, department_name FROM departments")
    departments = pg_cursor.fetchall()

    department_docs = []
    for department in departments:
        department_id, department_name = department
        department_docs.append({
            "_id": department_id,
            "department_name": department_name
        })

    if department_docs:
        mongo_db.departments.insert_many(department_docs)
        print(f"Departments migrated: {len(department_docs)}")

# Migrate Students
def migrate_students():
    pg_cursor.execute("SELECT student_id, student_name, student_email, department_id FROM students")
    students = pg_cursor.fetchall()

    student_docs = []
    for student in students:
        student_id, student_name, student_email, department_id = student
        student_docs.append({
            "_id": student_id,
            "student_name": student_name,
            "student_email": student_email,
            "department_id": department_id
        })

    if student_docs:
        mongo_db.students.insert_many(student_docs)
        print(f"Students migrated: {len(student_docs)}")

# Migrate Instructors
def migrate_instructors():
    pg_cursor.execute("SELECT instructor_id, instructor_name, instructor_email, department_id FROM instructors")
    instructors = pg_cursor.fetchall()

    instructor_docs = []
    for instructor in instructors:
        instructor_id, instructor_name, instructor_email, department_id = instructor
        instructor_docs.append({
            "_id": instructor_id,
            "instructor_name": instructor_name,
            "instructor_email": instructor_email,
            "department_id": department_id
        })

    if instructor_docs:
        mongo_db.instructors.insert_many(instructor_docs)
        print(f"Instructors migrated: {len(instructor_docs)}")

# Migrate Courses and assign Instructors
def migrate_courses():
    pg_cursor.execute('''
        SELECT c.course_id, c.course_code, c.department_id, c.is_core_course, c.offering_year, i.instructor_id 
        FROM courses c
        LEFT JOIN teaches t ON c.course_id = t.course_id
        LEFT JOIN instructors i ON t.instructor_id = i.instructor_id
    ''')
    courses = pg_cursor.fetchall()

    course_docs = []
    for course in courses:
        course_id, course_code, department_id, is_core_course, offering_year, instructor_id = course
        course_docs.append({
            "_id": course_id,
            "course_code": course_code,
            "department_id": department_id,
            "is_core_course": is_core_course,
            "offering_year": offering_year,
            "instructor_id": instructor_id  # Link to instructor who teaches the course
        })

    if course_docs:
        mongo_db.courses.insert_many(course_docs)
        print(f"Courses migrated: {len(course_docs)}")

# Migrate Enrollments and Embed in Students
def migrate_enrollments():
    pg_cursor.execute("SELECT student_id, course_id FROM enrollments")
    enrollments = pg_cursor.fetchall()

    student_enrollments = {}
    for enrollment in enrollments:
        student_id, course_id = enrollment
        if student_id not in student_enrollments:
            student_enrollments[student_id] = []
        student_enrollments[student_id].append(course_id)

    # Embed enrollments in students collection
    for student_id, courses in student_enrollments.items():
        mongo_db.students.update_one(
            {"_id": student_id},
            {"$set": {"enrolled_courses": courses}}
        )
    
    print(f"Enrollments embedded in students: {len(student_enrollments)}")

# Migration Process
def migrate():
    reset_mongo_collections()  # Drop collections to avoid duplicates
    migrate_departments()
    migrate_students()
    migrate_instructors()
    migrate_courses()
    migrate_enrollments()
    create_indexes()  # Create indexes after migration
    print("Migration complete with indexes!")

if __name__ == "__main__":
    migrate()

    # Close PostgreSQL connection
    pg_cursor.close()
    pg_conn.close()
