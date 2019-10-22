# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import elasticsearch
from elasticsearch import helpers
from elasticsearch import Elasticsearch
import json
import copy
import time
import codecs


es = Elasticsearch(timeout=300,max_retries=3)

# query
query_file_name = "Chemical_0112"

def query_title(name_list):

	query_all_pmid_result = {}
	pmid_abs = []
	pmid_no_abs = []

	for idx in range(len(name_list)):
			
		name = name_list[idx].strip()
		qbody = {
					"size" : 10000 ,
					"query":{
						"match": {
							"_title" : name
						}
					},

				}
		
		res = es.search(index = 'ai_medline_chemical', doc_type = "ai_medline_chemical_type", body = qbody)
		if len(res['hits']['hits']) > 0 :

			for i in range(len(res['hits']['hits'])) :
				if name == res['hits']['hits'][i]['_source']['_title'] :
					chemical_id = res['hits']['hits'][i]['_id']
					
					pmid_abs.append((name, chemical_id))
				else :
					pmid_no_abs.append(name)

		else :
			
			pmid_no_abs.append(name)

		if idx % 3000 == 0 :
			time.sleep(3)

	query_all_pmid_result['Find'] = pmid_abs
	query_all_pmid_result['Not_Find'] = list(set(pmid_no_abs))
	
	return query_all_pmid_result
	#--------------------------------------------------------------

def query_synonym(name_list):

	query_all_pmid_result = {}
	pmid_abs = []
	pmid_no_abs = []

	for idx in range(len(name_list)):
		
		name = name_list[idx].strip()
		qbody = {
					"size" : 10000 ,
					"query":{
						"match_phrase": {
							"_synonym" : name
						}
					},

				}
		
		res = es.search(index = 'ai_medline_chemical', doc_type = "ai_medline_chemical_type", body = qbody)

	
		if len(res['hits']['hits']) > 0 :
			for i in range(len(res['hits']['hits'])) :
				synonym = res['hits']['hits'][i]['_source']['_synonym']
				synonym_list = synonym.split(' , ')
				
				if name in synonym_list :
					chemical_id = res['hits']['hits'][i]['_id']
				
					pmid_abs.append((name, chemical_id))			
						
		else :
			
			pmid_no_abs.append(name)

		if idx % 3000 == 0 :
			time.sleep(3)

	query_all_pmid_result['Find'] = pmid_abs
	query_all_pmid_result['Not_Find'] = pmid_no_abs
	
	return query_all_pmid_result


import os

if __name__ =='__main__' :

	filename = "CHEBI/QUERY_NAME/" + query_file_name + ".txt"

	name_list = []
	with codecs.open(filename,'r', encoding='utf-8') as file_for_name :
		
		name_list = (line.strip() for line in file_for_name)
		name_list = list(line for line in name_list if line)

		file_for_name.close()
	t1 = time.time()
	
	search_res_title = query_title(name_list)
	
	search_res_synonym = query_synonym(name_list)
	#------------
	# add search_res_title and search_res_synonym in one dict
	all_search_res = dict()
	all_search_res['Find'] = list(set(search_res_title['Find'] + search_res_synonym['Find']))
	all_search_res['Not_Find_name'] = list(set(search_res_title['Not_Find'] ))
	all_search_res['Not_Find_synonym'] = list(set(search_res_synonym['Not_Find']))

	all_list = all_search_res['Find']
	all_dict = dict()
	for gene, gene_id in all_list :
		if gene not in all_dict:
			all_dict[gene] = list()
			all_dict[gene].append(int(gene_id))
		else :
			all_dict[gene].append(int(gene_id))
	
	gene_mini_id = dict()
	for gene in all_dict.keys():
		mini_id = all_dict[gene][0]
		gene_mini_id[gene] = mini_id
		
	print(filename, time.time() - t1)
	
	file_name = "CHEBI/SEARCH_ANS/" + query_file_name + ".json"
	with codecs.open( file_name ,'w', encoding='utf-8') as fout :
		json_gene_mini_id = json.dumps(gene_mini_id, indent =4)
		fout.write(json_gene_mini_id)
		fout.close()
