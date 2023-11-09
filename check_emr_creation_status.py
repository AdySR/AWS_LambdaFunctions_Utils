import json
import os
import boto3
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)
emr = boto3.client('emr')


def lambda_handler(event, context): 
    log.info("Lambda invoked for event : {0}".format(event))
    response = {}
    
    cluster_name = 'datalake-ingestion'
    
    cluster_id = get_cluster_id(cluster_name)
    
    response['clusterId'] = cluster_id
    #response['clusterId'] = 'abc_jkls123'
    print(response)
    return response
    
    
def get_cluster_id(name):

	log.info("Retrieving cluster id for {0}".format(name))
	cluster_iterator = emr.get_paginator('list_clusters').paginate(
							ClusterStates=[
								'STARTING','BOOTSTRAPPING','RUNNING','WAITING'
							]
						)
	for page in cluster_iterator:
		for cluster in page['Clusters']:
			if cluster['Name'].lower() == name.lower():
				return cluster['Id']
				
	return None
