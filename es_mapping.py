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






# res1 = mapping(data)
# for item in res1:
# 	# res2 = res1
# 	write(data,item)

# print(res2)
# print(res2[0])
# print('+++++++++')
# print(res2[1])
# print('========')
# print(res2[2])

# print(res2)
# write(data,res2)
	# write(data,res1)
# mapping(data)			
# for index_obj in data['index']:
	# index = index_obj.keys()[0]
	# file = index_obj[index]
# 	res=es.search(index=index, doc_type='jdbc', body=
# 	{
# 		'query':
# 			{
# 				'match_all':
# 					{}
# 			}
# 	}
# 	)	
# 	res1= es.indices.get_mapping(index=index, doc_type='jdbc')
# 	res1=ast.literal_eval(json.dumps(res1))
# 	X=res1.keys()
# 	res1 = res1[X[0]]['mappings']['jdbc']['properties']
# 	# print(X)
# 	# y=res1.values()
# 	# print(y)
# 	for i in res1:
# 		res1[i]=res1[i]['type']
# 	print(res1)
	# with open(file , 'a') as f:
		# writer = csv.DictWriter(
    	# f, fieldnames=["Column", "Type"])
		# writer.writeheader()
		# for key in res2.keys():
			# f.write("%s,%s\n"%(key,res2[key]))










# data 

# def fetch_data(index):
# 	res = es.search()
# 	return res

# def write(file, res):
# 	.......


# for index_obj in data....:
# 	res = fetch_data(index_obj)
# 	write(index_obj_file, res)