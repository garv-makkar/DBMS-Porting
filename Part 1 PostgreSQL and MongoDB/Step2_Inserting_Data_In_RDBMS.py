import psycopg2
import random
from faker import Faker

# Database connection details
db_name = "university-db"
db_user = "postgres"  # Replace with your PostgreSQL username
db_password = "Garv10"  # Replace with your PostgreSQL password
db_host = "localhost"  # Host of the PostgreSQL server
db_port = "5432"  # Default PostgreSQL port

fake = Faker()
random.seed(42)  # Random seed for consistency in generated data

# Insert departments
def insert_departments(cursor):
    departments = ["Computer Science", "Electrical", "Mechanical"]
    for dept in departments:
        cursor.execute("SELECT * FROM departments WHERE department_name = %s", (dept,))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO departments (department_name) VALUES (%s)", (dept,))
    print("Departments inserted.")

# Insert random instructors into each department
def insert_instructors(cursor):
    cursor.execute("SELECT department_id FROM departments")
    departments = cursor.fetchall()
    
    for dept in departments:
        department_id = dept[0]
        for _ in range(random.randint(3, 5)):  # Random number of instructors per department
            instructor_name = fake.name()
            instructor_email = fake.unique.email()
            cursor.execute("SELECT * FROM instructors WHERE instructor_email = %s", (instructor_email,))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO instructors (instructor_name, instructor_email, department_id) VALUES (%s, %s, %s)",
                    (instructor_name, instructor_email, department_id)
                )
    print("Instructors inserted.")

# Insert random courses into each department
def insert_courses(cursor):
    department_codes = {
        "Computer Science": "CS",
        "Electrical": "EE",
        "Mechanical": "ME"
    }

    cursor.execute("SELECT department_id, department_name FROM departments")
    departments = cursor.fetchall()
    
    for dept in departments:
        department_id = dept[0]
        department_name = dept[1]
        course_counter = 1  # Counter for unique course numbers
        
        for _ in range(random.randint(4, 6)):  # Random number of courses per department
            for year in range(2020, 2023):  # Courses offered in multiple years
                course_code = f"{department_codes[department_name]}-{course_counter:03d}"
                is_core_course = random.choice([True, False])
                cursor.execute("SELECT * FROM courses WHERE course_code = %s AND offering_year = %s", (course_code, year))
                if cursor.fetchone() is None:
                    cursor.execute(
                        "INSERT INTO courses (course_code, department_id, is_core_course, offering_year) VALUES (%s, %s, %s, %s)",
                        (course_code, department_id, is_core_course, year)
                    )
                course_counter += 1  # Increment course number for next course
    print("Courses inserted.")

# Insert random students into each department
def insert_students(cursor):
    cursor.execute("SELECT department_id FROM departments")
    departments = cursor.fetchall()
    
    for dept in departments:
        department_id = dept[0]
        for _ in range(random.randint(10, 15)):  # Random number of students per department
            student_name = fake.name()
            student_email = fake.unique.email()
            cursor.execute("SELECT * FROM students WHERE student_email = %s", (student_email,))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO students (student_name, student_email, department_id) VALUES (%s, %s, %s)",
                    (student_name, student_email, department_id)
                )
    print("Students inserted.")

# Assign instructors to teach courses (only one instructor per course)
def insert_teaches(cursor):
    cursor.execute("SELECT instructor_id, department_id FROM instructors")
    instructors = cursor.fetchall()

    for instructor in instructors:
        instructor_id, department_id = instructor
        
        # Get courses only from the same department as the instructor
        cursor.execute("SELECT course_id FROM courses WHERE department_id = %s", (department_id,))
        department_courses = cursor.fetchall()

        # Each instructor teaches 2-3 random courses from their own department, ensuring unique assignments
        for course in random.sample(department_courses, min(len(department_courses), 3)):
            course_id = course[0]
            cursor.execute("SELECT * FROM teaches WHERE course_id = %s", (course_id,))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO teaches (instructor_id, course_id) VALUES (%s, %s)",
                    (instructor_id, course_id)
                )
    print("Teaches assignments inserted.")

# Enroll students into courses from their department
def insert_enrollments(cursor):
    cursor.execute("SELECT student_id, department_id FROM students")
    students = cursor.fetchall()

    for student in students:
        student_id, department_id = student
        
        # Get courses only from the same department as the student
        cursor.execute("SELECT course_id FROM courses WHERE department_id = %s", (department_id,))
        department_courses = cursor.fetchall()

        # Each student enrolls in 3-5 random courses from their own department
        for course in random.sample(department_courses, min(len(department_courses), random.randint(3, 5))):
            course_id = course[0]
            cursor.execute("SELECT * FROM enrollments WHERE student_id = %s AND course_id = %s", (student_id, course_id))
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO enrollments (student_id, course_id) VALUES (%s, %s)",
                    (student_id, course_id)
                )
    print("Enrollments inserted.")

# Main function to run all inserts
def insert_data():
    try:
        conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        cursor = conn.cursor()

        insert_departments(cursor)
        insert_instructors(cursor)
        insert_courses(cursor)
        insert_students(cursor)
        insert_teaches(cursor)
        insert_enrollments(cursor)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    insert_data()
