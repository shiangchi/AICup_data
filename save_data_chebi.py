import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import os
import codecs

import sys
reload(sys)
sys.setdefaultencoding('utf8')
#-------------
path = "CHEBI/CHEBI.txt"

def test_streaming_bulk(file_name_list):

	count = 0
	actions=[]

	for file_name in file_name_list :
		
		with codecs.open(path+'/'+file_name,'r', encoding='utf-8') as file :
			content = ""
			content = file.readlines()
				
		meeting = content[0].split(':')[1].strip()
		title = content[1].split(':')[1].strip()
		abstract = content[-1].strip()
		filename = file_name.split('.')[0]

		
		actions.append({
						
						'_index': "medline",
						'_type': 'medline_type',
						'_id': filename ,
						'_title' : str(title) ,
						'_abstract': str(abstract),
						'_meshheadinglist' : 'None',
						'_chemicallist' : 'None',
						'_otherabst' : 'None',
						'_keyword': 'None',

						})
	
	
	return actions

def gene_info(path):
	content = []
	actions=[]
	for line in codecs.open(path,'r', encoding='utf-8') :
		chemical_id = line.split('\t')[0].strip()
		chemical_name = line.split('\t')[1].strip()
		synonym = ""
		synonym_list = line.split('\t')[2::]
		if len(synonym_list) > 1 :
			for i in range(len(synonym_list)) :
				synonym += synonym_list[i].strip()+' , '
		if len(synonym_list) == 1 :
			synonym += synonym_list[0].strip()	

		actions.append({
			
			'_index': "ai_medline_chemical",
			'_type': 'ai_medline_chemical_type',
			'_id': chemical_id ,
			'_title' : chemical_name ,
			'_synonym': synonym,
			

			})

	return actions


if __name__ == '__main__':
	
	iternb = 20000

	actions = gene_info(path)
	print("actions",len(actions))
	t1 = time.time()
	es = Elasticsearch(timeout=600, max_retries=3, connection_class=elasticsearch.RequestsHttpConnection)
	success, failed = 0, 0
	
	for i in range(0,len(actions),iternb):
		t1 = time.time()
		for ok, item in helpers.streaming_bulk(es, actions[i:i+iternb], chunk_size=iternb, max_chunk_bytes=57286400) :
			if not ok:
				failed += 1
			else:
				success += 1
		
		time.sleep(.5)
		t2 = time.time()
		print('used time: {}'.format(t2-t1))
