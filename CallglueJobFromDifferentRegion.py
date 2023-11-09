import json
import boto3

def call_data_profiler_generic(event, context):
    client_glue = boto3.client('glue', region_name='us-west-2')
    response = client_glue.start_job_run(JobName ='data_profiler_generic')
    print(response)
