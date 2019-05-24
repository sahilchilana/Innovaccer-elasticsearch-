import pymongo

class MongoClient:
	def __init__(self):  
		self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
		self.mydb = self.myclient["mydatabase"]
		self.mycol = self.mydb["config_file"]

	def find(self):
		return self.mycol.find({},{"_id":0})[0]