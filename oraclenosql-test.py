from borneo import (
    DeleteRequest, NoSQLHandle, NoSQLHandleConfig, PutRequest,
    QueryRequest, TableRequest)
from borneo.kv import StoreAccessTokenProvider
import time
from faker import Faker
import random
import matplotlib.pyplot as plt
import pandas as pd

# Initialize Faker
fake = Faker()

# Constants for the amounts of data to generate
NUM_USERS = 10
NUM_COURSES = 20
NUM_LESSONS_PER_COURSE = 5
NUM_QUIZZES_PER_LESSON = 2
NUM_QUESTIONS_PER_QUIZ = 3
NUM_ENROLLMENTS_PER_USER = 2

# Oracle NoSQL Database endpoint
kvstore_endpoint = 'localhost:8080'

timings = []

def edit_number_of_operations(multiplication=1):
    global NUM_USERS, NUM_COURSES, NUM_ENROLLMENTS_PER_USER, NUM_LESSONS_PER_COURSE, NUM_QUIZZES_PER_LESSON, NUM_QUESTIONS_PER_QUIZ

    NUM_USERS = NUM_USERS * multiplication
    NUM_COURSES = NUM_COURSES * multiplication
    # NUM_ENROLLMENTS_PER_USER = NUM_ENROLLMENTS_PER_USER * multiplication
    # NUM_LESSONS_PER_COURSE = NUM_LESSONS_PER_COURSE * multiplication
    # NUM_QUIZZES_PER_LESSON = NUM_QUIZZES_PER_LESSON * multiplication
    # NUM_QUESTIONS_PER_QUIZ = NUM_QUESTIONS_PER_QUIZ * multiplication

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
    timings.append((operation_name, duration))
    print(f"{operation_name} took {duration:.2f} seconds")

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

def insert_enrollments(handle, users, courses):
    for user in users:
        if user["role"] == "student":
            enrolled_courses = random.sample(courses, NUM_ENROLLMENTS_PER_USER)
            user["enrolledCourses"] = [course["id"] for course in enrolled_courses]
            user_request = PutRequest().set_table_name("Users").set_value(user)
            handle.put(user_request)
            for course in enrolled_courses:
                enrollment = {
                    "id": fake.uuid4(),
                    "userId": user["id"],
                    "courseId": course["id"],
                    "enrollmentDate": fake.date_time_this_year(),
                    "progress": random.choice(["not started", "in progress", "completed"])
                }
                enrollment_request = PutRequest().set_table_name("Enrollments").set_value(enrollment)
                handle.put(enrollment_request)
    print("Enrollments inserted successfully")

def retrieve_enrollments(handle):
    enrollments = []
    query = 'SELECT * FROM Enrollments'
    query_request = QueryRequest().set_statement(query)
    query_result = handle.query(query_request)
    results = query_result.get_results()
    for result in results:
        enrollments.append(dict(result))
    print(f"{len(enrollments)} enrollments retrieved successfully")
    return enrollments

def insert_all_data(handle):
    start_time_users = time.time()
    insert_users(handle)
    stop_time_users = time.time()

    users = retrieve_users(handle)

    start_time_courses = time.time()
    insert_courses(handle, users)
    stop_time_courses = time.time()

    courses = retrieve_courses(handle)

    start_time_enrollments = time.time()
    insert_enrollments(handle, users, courses)
    stop_time_enrollments = time.time()

    duration = stop_time_users - start_time_users + stop_time_courses - start_time_courses + stop_time_enrollments - start_time_enrollments
    print(f"Inserting all data took {duration:.2f} seconds")

def retrieve_all_data(handle):
    query_tables = ["Users", "Courses", "Lessons", "Quizzes", "Questions", "Enrollments"]
    for table in query_tables:
        query = f'SELECT * FROM {table}'
        query_request = QueryRequest().set_statement(query)
        handle.query(query_request)
    print("All data retrieved successfully")

def update_all_data(handle):
    query_tables = ["Users", "Courses", "Lessons", "Quizzes", "Questions", "Enrollments"]
    for table in query_tables:
        query = f'SELECT * FROM {table}'
        query_request = QueryRequest().set_statement(query)
        query_result = handle.query(query_request)
        results = query_result.get_results()
        for result in results:
            record = dict(result)
            if table == "Users":
                record["name"] = record["name"] + "_updated"
            elif table == "Courses":
                record["title"] = record["title"] + "_updated"
            elif table == "Lessons":
                record["title"] = record["title"] + "_updated"
            elif table == "Quizzes":
                record["title"] = record["title"] + "_updated"
            elif table == "Questions":
                record["text"] = record["text"] + "_updated"
            elif table == "Enrollments":
                record["progress"] = record["progress"] + "_updated"
            put_request = PutRequest().set_table_name(table).set_value(record)
            handle.put(put_request)
    print("All data updated successfully")

def delete_all_data(handle):
    tables = ["Users", "Courses", "Lessons", "Quizzes", "Questions", "Enrollments"]
    for table in tables:
        query = f'SELECT * FROM {table}'
        query_request = QueryRequest().set_statement(query)
        query_result = handle.query(query_request)
        results = query_result.get_results()
        for result in results:
            record = dict(result)
            delete_request = DeleteRequest().set_table_name(table).set_key({"id": record["id"]})
            handle.delete(delete_request)
    print("All data deleted successfully")

def plot_timings():
    operations, durations = zip(*timings)

    plt.figure(figsize=(12, 6))
    plt.barh(operations, durations, color='skyblue')
    plt.xlabel('Time (seconds)')
    plt.title('Performance of OracleNoSQL Database Operations')
    plt.grid(axis='x')
    plt.show()

def save_timings_to_excel(filename="timings_.xlsx"):
    timings_df = pd.DataFrame(timings, columns=["Operation", "Duration (seconds)"])
    timings_df.to_excel(filename, index=False)
    print(f"Timings saved to {filename} successfully")

def main():
    handle = None
    try:
        # Create a handle
        handle = get_handle()
    
        multiplication = input("Enter how many times to multiply the amount of data: ")
        if not isinstance(multiplication, int):
            print("Provided multiplication factor is not an integer. Defaulting to 1.")
            multiplication = 1
        edit_number_of_operations(multiplication)

        measure_time("Drop Tables", lambda: drop_tables(handle))
        measure_time("Create Tables", lambda: create_tables(handle))
        measure_time("Insert All Data", lambda: insert_all_data(handle))
        measure_time("Retrieve All Data", lambda: retrieve_all_data(handle))
        measure_time("Update All Data", lambda: update_all_data(handle))
        measure_time("Delete All Data", lambda: delete_all_data(handle))

        print('Performance test completed')

        save_timings_to_excel("timings_OracleNoSQL_{}.xlsx".format(multiplication))
        plot_timings()

    except Exception as e:
        print(e)
    finally:
        # If the handle isn't closed Python will not exit properly
        if handle is not None:
            handle.close()

if __name__ == '__main__':
    main()
