import json
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["config_file"]

with open('config.json') as f:
    file_data = json.load(f)

mycol.insert_one(file_data)