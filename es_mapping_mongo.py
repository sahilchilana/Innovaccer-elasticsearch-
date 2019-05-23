from elasticsearch import Elasticsearch 
import openpyxl
import csv,ast,json
import pymongo

def write(index,res):   
	with open(index , 'a') as f:
		writer = csv.DictWriter(
		f, fieldnames=["Column", "Type"])
		writer.writeheader()
		for key in res.keys():
			f.write("%s,%s\n"%(key,res[key]))
	return f

class MongoClient:
	def __init__(self):  
		self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
		self.mydb = self.myclient["mydatabase"]
		self.mycol = self.mydb["config_file"]

	def find(self):
		return self.mycol.find({},{"_id":0})[0]

class ES:
	def __init__(self):
		self.es = Elasticsearch([{'host':'localhost','port':9200}])
	
	def mapping(self, index): 
		res = self.es.indices.get_mapping(index=index, doc_type='jdbc')
		res = ast.literal_eval(json.dumps(res))
		X = res.keys()
		res = res[X[0]]['mappings']['jdbc']['properties']
		for i in res:
			res[i]=res[i]['type']
		return res



def main():
	mg = MongoClient()
	print(mg.myclient)
	config = mg.find()['index']
	print(config)
	# for item in config:
	# 	print(item)
	for index_obj in config :
		index = index_obj.keys()[0]
		file = index_obj[index]
		# print index
		# print file
		res1=ES()
		res=res1.mapping(index)
		# print res
		write(file,res)

if __name__=="__main__":
	main()