from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['college_db']
students = db['students']

student = {
    "student_id": "STU-101",
    "name": "RISHI KUMAR",
    "subjects": ["DBMS", "JAVA", "DAA"],
    "grades": [
        {"subject": "DBMS", "grade": "A"},
        {"subject": "DAA", "grade": "B+"}
    ],
    "attendance_percentage": 92.5
}

result = students.insert_one(student)
print(f"Inserted student with ID: {result.inserted_id}")

client.close()
