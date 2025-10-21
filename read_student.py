from pymongo import MongoClient
from pprint import pprint

client = MongoClient('mongodb://localhost:27017/')
db = client['college_db']
students = db['students']

student = students.find_one({"student_id": "STU-101"})
if student:
    pprint(student)
else:
    print("Student not found.")

client.close()
