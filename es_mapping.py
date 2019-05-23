from elasticsearch import Elasticsearch 
import openpyxl
import csv,ast,json
es=Elasticsearch([{'host':'localhost','port':9200}])

def json_file():
	with open('config.json') as json_data_file:
		data = json.load(json_data_file)
	return data

def mapping(index):
	res = es.indices.get_mapping(index=index, doc_type='jdbc')
	res = ast.literal_eval(json.dumps(res))
	X = res.keys()
	res = res[X[0]]['mappings']['jdbc']['properties']
	for i in res:
		res[i]=res[i]['type']
	return res

def write(index,res):	
	with open(index , 'a') as f:
		writer = csv.DictWriter(
		f, fieldnames=["Column", "Type"])
		writer.writeheader()
		for key in res.keys():
			f.write("%s,%s\n"%(key,res[key]))
	return f




data = json_file()
for index_obj in data['index']:
	index = index_obj.keys()[0]
	file = index_obj[index]
	res=mapping(index)
	write(file,res)
