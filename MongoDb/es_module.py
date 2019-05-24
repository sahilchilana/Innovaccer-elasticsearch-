from elasticsearch import Elasticsearch 
import csv,ast,json

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