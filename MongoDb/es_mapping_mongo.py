from elasticsearch import Elasticsearch 
import csv,ast,json
import pymongo
import mongo_client
import es_module

def write(index,res):   
	with open(index , 'a') as f:
		writer = csv.DictWriter(
		f, fieldnames=["Column", "Type"])
		writer.writeheader()
		for key in res.keys():
			f.write("%s,%s\n"%(key,res[key]))
	return f

def main():
	mg = mongo_client.MongoClient()
	es = es_module.ES()
	config = mg.find()['index']
	for index_obj in config :
		index = index_obj.keys()[0]
		file = index_obj[index]
		res=es.mapping(index)
		write(file,res)

if __name__=="__main__":
	main()
