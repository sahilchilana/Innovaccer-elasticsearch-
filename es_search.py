from elasticsearch import Elasticsearch 
import openpyxl
import csv,ast,json
es=Elasticsearch([{'host':'localhost','port':9200}])

def json_file():
	with open('config.json') as json_data_file:
		data = json.load(json_data_file)
	return data
def search(index):
	res=es.search(index=index, doc_type='jdbc', body=
	{
		'query':
			{
				'match_all':
					{}
			}
	}
	)
	return res
data = json_file()
for index_obj in data['index']:
	index = index_obj.keys()[0]
	res=search(index)
		