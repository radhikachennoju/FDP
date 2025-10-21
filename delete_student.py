from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['college_db']
students = db['students']

# Step 1: Delete a specific student document
delete_result = students.delete_one({"student_id": "STU-101"})
print(f"Documents deleted from students collection: {delete_result.deleted_count}")

# Step 2: Delete the entire database (drops all collections in 'college_db')
client.drop_database('college_db')
print("Database 'college_db' deleted successfully!")

client.close()