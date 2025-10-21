from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['college_db']
students = db['students']

# Example: add a new subject
update_result = students.update_one(
    {"student_id": "STU-101"},
    {"$push": {"subjects": "SDE"}}
)

print(f"Documents updated: {update_result.modified_count}")

client.close()
