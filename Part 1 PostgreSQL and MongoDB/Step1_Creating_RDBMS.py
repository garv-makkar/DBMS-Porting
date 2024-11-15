import psycopg2
from psycopg2 import sql

# Database connection details
db_name = ""
db_user = ""  # Replace with your PostgreSQL username
db_password = ""  # Replace with your PostgreSQL password
db_host = ""  # Host of the PostgreSQL server
db_port = ""  # Default PostgreSQL port

# Create tables with unique instructor-course assignments
def create_tables():
    try:
        conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        cursor = conn.cursor()

        # Create Departments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS departments (
                department_id SERIAL PRIMARY KEY,
                department_name VARCHAR(100) NOT NULL
            );
        ''')

        # Create Students table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                student_id SERIAL PRIMARY KEY,
                student_name VARCHAR(100) NOT NULL,
                student_email VARCHAR(100) UNIQUE NOT NULL,
                department_id INTEGER NOT NULL,
                FOREIGN KEY (department_id) REFERENCES departments(department_id)
            );
        ''')

        # Create Instructors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS instructors (
                instructor_id SERIAL PRIMARY KEY,
                instructor_name VARCHAR(100) NOT NULL,
                instructor_email VARCHAR(100) UNIQUE NOT NULL,
                department_id INTEGER NOT NULL,
                FOREIGN KEY (department_id) REFERENCES departments(department_id)
            );
        ''')

        # Create Courses table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS courses (
                course_id SERIAL PRIMARY KEY,
                course_code VARCHAR(20) UNIQUE NOT NULL,
                department_id INTEGER NOT NULL,
                is_core_course BOOLEAN NOT NULL,
                offering_year INTEGER NOT NULL,
                FOREIGN KEY (department_id) REFERENCES departments(department_id)
            );
        ''')

        # Create Teaches table (with unique constraint on course_id)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS teaches (
                teaches_id SERIAL PRIMARY KEY,
                instructor_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                FOREIGN KEY (instructor_id) REFERENCES instructors(instructor_id),
                FOREIGN KEY (course_id) REFERENCES courses(course_id),
                UNIQUE (course_id)  -- Ensure only one instructor per course
            );
        ''')

        # Create Enrollments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrollments (
                enrollment_id SERIAL PRIMARY KEY,
                student_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students(student_id),
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            );
        ''')

        conn.commit()
        cursor.close()
        conn.close()
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_tables()  # Create tables if they do not exist
