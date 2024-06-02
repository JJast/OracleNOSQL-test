from borneo import (
    DeleteRequest, GetRequest, NoSQLHandle, NoSQLHandleConfig, PutRequest,
    QueryRequest, TableLimits, TableRequest, NoSQLException)
from borneo.kv import StoreAccessTokenProvider
import time
from faker import Faker
import random
import matplotlib.pyplot as plt

# Initialize Faker
fake = Faker()

# Constants for the amounts of data to generate
NUM_USERS = 200
NUM_COURSES = 50
NUM_ENROLLMENTS_PER_USER = 2
NUM_LESSONS_PER_COURSE = 5
NUM_QUIZZES_PER_LESSON = 2
NUM_QUESTIONS_PER_QUIZ = 3

# Oracle NoSQL Database endpoint
kvstore_endpoint = 'localhost:8080'

def get_handle():
    print('Using on-premise endpoint ' + kvstore_endpoint)
    endpoint = kvstore_endpoint
    provider = StoreAccessTokenProvider()
    return NoSQLHandle(NoSQLHandleConfig(endpoint, provider))

def measure_time(operation_name, func):
    start_time = time.time()
    func()
    end_time = time.time()
    duration = end_time - start_time
    print(f"{operation_name} took {duration:.2f} seconds")
    return duration

def create_tables(handle):
    statements = [
        'CREATE TABLE IF NOT EXISTS Users (id STRING, name STRING, email STRING, role STRING, enrolledCourses ARRAY(STRING), PRIMARY KEY(id))',
        'CREATE TABLE IF NOT EXISTS Courses (id STRING, title STRING, description STRING, instructor STRING, lessons ARRAY(STRING), enrollments ARRAY(STRING), PRIMARY KEY(id))',
        'CREATE TABLE IF NOT EXISTS Lessons (id STRING, courseId STRING, title STRING, content STRING, quizzes ARRAY(STRING), PRIMARY KEY(id))',
        'CREATE TABLE IF NOT EXISTS Quizzes (id STRING, lessonId STRING, title STRING, questions ARRAY(STRING), PRIMARY KEY(id))',
        'CREATE TABLE IF NOT EXISTS Questions (id STRING, quizId STRING, text STRING, options ARRAY(STRING), correctAnswer STRING, PRIMARY KEY(id))',
        'CREATE TABLE IF NOT EXISTS Enrollments (id STRING, userId STRING, courseId STRING, enrollmentDate TIMESTAMP(3), progress STRING, PRIMARY KEY(id))'
    ]

    for statement in statements:
        request = TableRequest().set_statement(statement)
        handle.do_table_request(request, 40000, 3000)
    print("Tables created successfully")

def drop_tables(handle):
    tables = ["Users", "Courses", "Lessons", "Quizzes", "Questions", "Enrollments"]
    for table in tables:
        drop_statement = f'DROP TABLE IF EXISTS {table}'
        drop_request = TableRequest().set_statement(drop_statement)
        handle.do_table_request(drop_request, 40000, 3000)
    print("Tables dropped successfully")

def insert_users(handle):
    user_request = PutRequest().set_table_name("Users")
    for _ in range(NUM_USERS):
        user = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "role": random.choice(["student", "instructor"]),
            "enrolledCourses": []
        }
        user_request.set_value(user)
        handle.put(user_request)
    print("Users inserted successfully")

def retrieve_users(handle):
    users = []
    query = 'SELECT * FROM Users'
    query_request = QueryRequest().set_statement(query)
    query_result = handle.query(query_request)

    results = query_result.get_results()

    for result in results:
        users.append(dict(result))
    print("Users retrieved successfully")
    
    return users

def insert_courses(handle, users):
    for _ in range(NUM_COURSES):
        course = {
            "id": fake.uuid4(),
            "title": fake.catch_phrase(),
            "description": fake.text(),
            "instructor": random.choice([user["id"] for user in users if user["role"] == "instructor"]),
            "lessons": [],
            "enrollments": []
        }

        # Generate Lessons for each course
        for _ in range(NUM_LESSONS_PER_COURSE):
            lesson = {
                "id": fake.uuid4(),
                "courseId": course["id"],
                "title": fake.sentence(),
                "content": fake.text(),
                "quizzes": []
            }

            # Generate Quizzes for each lesson
            for _ in range(NUM_QUIZZES_PER_LESSON):
                quiz = {
                    "id": fake.uuid4(),
                    "lessonId": lesson["id"],
                    "title": fake.sentence(),
                    "questions": []
                }

                # Generate Questions for each quiz
                for _ in range(NUM_QUESTIONS_PER_QUIZ):
                    question = {
                        "id": fake.uuid4(),
                        "quizId": quiz["id"],
                        "text": fake.sentence(),
                        "options": [fake.word() for __ in range(4)],
                        "correctAnswer": fake.word()
                    }
                    quiz["questions"].append(question["id"])
                    # Insert question into Questions table
                    question_request = PutRequest().set_table_name("Questions").set_value(question)
                    handle.put(question_request)
                lesson["quizzes"].append(quiz["id"])
                # Insert quiz into Quizzes table
                quiz_request = PutRequest().set_table_name("Quizzes").set_value(quiz)
                handle.put(quiz_request)
            course["lessons"].append(lesson["id"])
            # Insert lesson into Lessons table
            lesson_request = PutRequest().set_table_name("Lessons").set_value(lesson)
            handle.put(lesson_request)
        # Insert course into Courses table
        course_request = PutRequest().set_table_name("Courses").set_value(course)
        handle.put(course_request)
    print("Courses, lessons, quizzes, and questions inserted successfully")

def retrieve_courses(handle):
    courses = []
    query = 'SELECT * FROM Courses'
    query_request = QueryRequest().set_statement(query)
    query_result = handle.query(query_request)
    results = query_result.get_results()
    for result in results:
        courses.append(dict(result))
    print(f"{len(courses)} courses retrieved successfully")
    return courses

def bulk_insert_users(handle):
    user_request = PutRequest().set_table_name("Users")
    for _ in range(NUM_USERS):
        user = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "role": random.choice(["student", "instructor"]),
            "enrolledCourses": []
        }
        user_request.set_value(user)
        handle.put(user_request)
    print("Bulk users inserted successfully")

def bulk_edit_users(handle):
    user_request = PutRequest().set_table_name("Users")
    query = 'SELECT * FROM Users'
    query_request = QueryRequest().set_statement(query)
    query_result = handle.query(query_request)

    results = query_result.get_results()

    for result in results:
        user = dict(result)
        user["name"] = fake.name()
        user_request.set_value(user)
        handle.put(user_request)
    print("Bulk users edited successfully")

def main():
    handle = None
    durations = {}
    try:
        # Create a handle
        handle = get_handle()

        durations["Drop Tables"] = measure_time("Drop Tables", lambda: drop_tables(handle))
        durations["Create Tables"] = measure_time("Create Tables", lambda: create_tables(handle))
        durations["Insert Users"] = measure_time("Insert Users", lambda: insert_users(handle))
        users = []
        durations["Retrieve Users"] = measure_time("Retrieve Users", lambda: users.extend(retrieve_users(handle)))
        
        # Main Create
        durations["Insert Courses, Lessons, Quizzes, and Questions"] = measure_time("Insert Courses, Lessons, Quizzes, and Questions", lambda: insert_courses(handle, users))
        # Main Retrive
        durations["Retrieve Courses"] = measure_time("Retrieve Courses", lambda: retrieve_courses(handle))
        
        # Create many
        durations["Bulk Insert Users"] = measure_time("Bulk Insert Users", lambda: bulk_insert_users(handle))
        # Update many
        durations["Bulk Edit Users"] = measure_time("Bulk Edit Users", lambda: bulk_edit_users(handle))

        print('Performance test completed')

        # Plotting the results
        plt.figure(figsize=(12, 6))
        plt.barh(durations.keys(), durations.values(), color='skyblue')
        plt.xlabel('Time (seconds)')
        plt.title('Performance of Oracle NoSQL Database Operations')
        plt.grid(axis='x')
        plt.show()

    except Exception as e:
        print(e)
    finally:
        # If the handle isn't closed Python will not exit properly
        if handle is not None:
            handle.close()

if __name__ == '__main__':
    main()
