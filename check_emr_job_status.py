import boto3
import logging
from collections import defaultdict


log = logging.getLogger()
log.setLevel(logging.INFO)

emr = boto3.client('emr', region_name='ap-south-1')
s3 = boto3.client('s3', region_name='ap-south-1')

FINAL_STATE = 'COMPLETED'
ERROR_STATUS = ['CANCEL_PENDING', 'CANCELLED', 'FAILED', 'INTERRUPTED']


# {'Result': [{'steps': [{'status': 'success', 'step_id': 's-2X5NYVXYW103A'}]}]}

def lambda_handler(event, context):
    print(event)

    job_files = event['Result']
    cluster_name = 'datalake-ingestion'
    cluster_id = get_cluster_id(cluster_name)
    submitted_steps = []
    submitted_steps_detail = {}

    for job in job_files:
        for step in job['steps']:
            if step['status'] == 'success':
                submitted_steps.append(step['step_id'])
                submitted_steps_detail[step['step_id']] = step

    current_step_status = check_step_status(cluster_id, submitted_steps)
    print(FINAL_STATE)

    errro_step = len(current_step_status['ERROR'])
    pending_step = len(current_step_status['PENDING'])
    running_step = len(current_step_status['RUNNING'])
    finished_step = len(current_step_status['COMPLETED'])
    submitted_step = len(submitted_steps)

    response = {}

    if pending_step > 0:
        response['completed'] = False
    elif errro_step > 0:
        response['completed'] = True
        response['error'] = True

    elif FINAL_STATE == 'COMPLETED':
        if finished_step == submitted_step:
            response['completed'] = True
        elif running_step > 0:
            response['completed'] = False
        else:
            response['completed'] = True

    return response


def check_step_status(cluster_id, steps):
    step_status_current = defaultdict(list)

    for index in range(0, len(steps), 10):

        emr_response = emr.list_steps(
            ClusterId=cluster_id,
            StepIds=steps[index:index + 10]
        )

        for step in emr_response['Steps']:
            step_state = step['Status']['State']
            if step_state in ERROR_STATUS:
                step_status_current['ERROR'].append(step['Id'])
            else:
                step_status_current[step_state].append(step['Id'])

    return step_status_current


def get_cluster_id(name):
    cluster_iterator = emr.get_paginator('list_clusters').paginate(
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )
    log.info(cluster_iterator)
    for page in cluster_iterator:
        for cluster in page['Clusters']:
            if cluster['Name'].lower() == name.lower():
                return cluster['Id']

    return None

#
# lambda_handler({'Result': [{'steps': [{'status': 'success', 'step_id': 's-3GOLO70INAHA0'}]}]}, None)
#
# {"Result": [{"steps": [{"status": "success", "step_id": "s-3GOLO70INAHA0"}]}]}
