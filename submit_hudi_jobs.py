import json
import logging
import random
import time

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client('s3', region_name='ap-south-1')
emr = boto3.client('emr', region_name='ap-south-1')

S3_HUDI_BUCKET = 'iris-datalake-hudi-ingestion'
S3_PREFIX = 's3://'

cluster_name = 'datalake-ingestion'


ADD_SPARK_JARS = [
    '--jars', '/usr/lib/spark/external/lib/spark-avro.jar',
    '/usr/lib/hudi/hudi-utilities-bundle.jar',
    '--table-type', 'COPY_ON_WRITE'
]

CONFIG_SPARK_BASIC = [
    'spark-submit', '--deploy-mode', 'cluster', "--class",
    'org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer',
]


def lambda_handler(event, context):
    log.info("event: {0}".format(event))

    response = {}
    jobs_status = []

    cluster_name = 'datalake-ingestion'
    cluster_id = fetch_cluster_id(cluster_name)

    for job in event['jobs_to_submit']:
        runtime_job_info = {'job-status': {}, "clusterId": cluster_id}

        log.info("processing job ->: {0}".format(job))

        job_status = {}
        job_file_name = job.get("job_file_name", None)
        job_status['job_file_name'] = job_file_name
        runtime_job_info['job_file_name'] = job_file_name

        runtime_job_info['cluster'] = cluster_name
        master_node_ips = find_master_node_ip(cluster_id)
        runtime_job_info['cluster_ips'] = master_node_ips

        try:
            table_list = load_tables_to_ingest_from_jobfile(job)
        except Exception as ex:
            err_msg = "Got exception Skipping file for processing: " + str(ex)
            job_status['error'] = err_msg
            return
        #
        # table_list = [{'job_source': 'DFS', 'job_type': 'FULL', 'op': 'INSERT', 'table_name': 'MUTUAL_FUND',
        #                'dfs_source_key': 'LANDING_DATA/MUTUAL_FUND',
        #                'hudi_property_key': 'RESOURCES/FULL/MUTUAL_FUND/NO_PARTITION/mutual_fund_full.properties',
        #                'hudi_target_key': 'RAW_HUDI_DATA/MUTUAL_FUND', 'glue_db_name': 'iris_hudi_raw_datalake'}]

        steps_status = []
        spark_submit_steps = []
        log.info("Iterating over finalize list of tables for processing")

        for table in table_list:
            try:
                step_details = {'step-status': {}}
                table_name = table['table_name'].lower()
                step_details['tbl_name'] = table_name
                step_details['job_type'] = table['job_type'].lower()
                step_details['step_name'] = '_'.join([table_name, table['job_type'].lower()])
                step_details['job_source'] = table['job_source'].lower()
                step_details['op'] = table['op']
                step_details['glue_db_name'] = 'iris_hudi_raw_datalake'
                step_details['hudi_property_key'] = table['hudi_property_key']
                step_details['hudi_target_key'] = table['hudi_target_key']
                master_ip = random.choice(runtime_job_info['cluster_ips'])
                step_details['master_ip'] = master_ip

                hive_jdbc_url = 'jdbc:hive2://' + master_ip + ':' + '10000'
                step_details['hive_jdbc_url'] = hive_jdbc_url

                # set source table configs
                set_source_table_level_configs(table, step_details)

                spark_submit_conf = {}
                set_spark_job_configurations(step_details, spark_submit_conf)

                # Creating final spark-submit command from all different configuration list

                log.info("Creating final spark-submit args:: ")
                spark_submit_args = CONFIG_SPARK_BASIC + spark_submit_conf['spark_config'] + \
                                    ADD_SPARK_JARS + spark_submit_conf['hudi_configs'] + spark_submit_conf[
                                        'hudi_src_data_config']

                log.info("Final spark-submit command: {0}".format(spark_submit_args))
                print("Final spark-submit command: {0}".format(spark_submit_args))

                step_submit = {
                    'Name': step_details['step_name'],
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': spark_submit_args
                    }
                }

                spark_submit_steps.append(step_submit)
                step_details['step-status']['status'] = 'submitted'
                steps_status.append(step_details['step-status'])

            except Exception as ex:
                err_msg = "Error: Unexpected error occurred: Reason:-> " + str(ex)
                step_details['step-status']['error'] = err_msg
                step_details['step-status']['status'] = 'fail'
                log.error(err_msg)
                steps_status.append(step_details['step-status'])

        step_response = emr.add_job_flow_steps(JobFlowId=runtime_job_info["clusterId"], Steps=spark_submit_steps)
        update_step_status_with_stepid(steps_status, step_response)
        print(steps_status)
        runtime_job_info['job-status']['steps'] = steps_status
        jobs_status.append(runtime_job_info['job-status'])
        time.sleep(1)

    response['Result'] = jobs_status
    return response


def load_tables_to_ingest_from_jobfile(job):
    job_file_s3key = job.get("job_file_name", None)

    if job_file_s3key is None:
        raise Exception("'job_file_name' not found in job processing json file")

    print("reading job file data")
    job_file_data = read_s3_json_file(S3_HUDI_BUCKET, job_file_s3key)
    if job_file_data is None:
        raise Exception("Error in reading file @: {0} from bucket".format(job_file_s3key))
    print("reading job file data done")
    tables_to_ingest = job_file_data.get("steps", None)
    if tables_to_ingest is None:
        raise Exception("not found 'steps' in json file: {0}".format(job_file_s3key))

    return tables_to_ingest


def find_master_node_ip(cluster_id):
    try:
        master_ips = []
        response = emr.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=[
                'MASTER'
            ],
            InstanceStates=[
                'AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING'
            ]
        )
        if response['Instances']:
            for master_node in response['Instances']:
                master_ips.append(master_node['PrivateIpAddress'])

        return master_ips
    except Exception as ex:
        log.error("Error: got an unexpected exception while retrieving master node ips " + str(ex))
        return None


def get_s3_object_data(bucket, key):
    try:
        log.info("Getting s3 object with bucket:{0} and key:{1}".format(bucket, key))
        s3_obj = s3.get_object(Bucket=bucket, Key=key)
        s3_data = s3_obj['Body'].read().decode('utf-8')
        return s3_data
    except Exception as ex:
        log.error("Error: got an unexpected exception while retrieving s3 object " + str(ex))
        return None


def read_s3_json_file(bucket, key):
    try:
        s3_data = get_s3_object_data(bucket, key)
        json_data = json.loads(s3_data)
        return json_data
    except Exception as ex:
        log.error("Error: got an unexpected exception while reading s3 json object " + str(ex))
        return None


def get_s3_path(bucket, key):
    log.info("Creating s3 uri for bucket: {0} and key: {1}".format(bucket, key))
    return S3_PREFIX + bucket + '/' + key


def set_source_specific_props(table, runtime_step_info):
    if runtime_step_info['job_source'] == 'dfs':
        source_key = table['dfs_source_key']
        source_path = S3_PREFIX + S3_HUDI_BUCKET + '/' + source_key
        runtime_step_info['source_path'] = source_path
        source_class = 'org.apache.hudi.utilities.sources.ParquetDFSSource'
        # in case of Csv raw files it will choose csv source class
        source_data_type = table.get('dfs_source_data_type', 'parquet').lower()
        if source_data_type == "csv":
            source_class = 'org.apache.hudi.utilities.sources.CsvDFSSource'
            source_schema_path = get_s3_path(S3_HUDI_BUCKET, table['source_schema'])
            target_schema_path = get_s3_path(S3_HUDI_BUCKET, table['target_schema'])
            runtime_step_info['source_schema_path'] = source_schema_path
            runtime_step_info['target_schema_path'] = target_schema_path

        runtime_step_info['source_class'] = source_class
        runtime_step_info['source_data_type'] = source_data_type


def set_source_table_level_configs(table, step_details):
    try:
        property_file_path = get_s3_path(S3_HUDI_BUCKET, table['hudi_property_key'])
        step_details['property_file_path'] = property_file_path

        target_path = get_s3_path(S3_HUDI_BUCKET, table['hudi_target_key'])
        step_details['target_path'] = target_path

        set_source_specific_props(table, step_details)

    except Exception as ex:
        err_msg = "Got exception while validating Reason:-> " + str(ex)
        log.error(err_msg)


def get_source_specific_conf(runtime_step_info):
    src_specific_conf = []
    if runtime_step_info['job_source'] == 'dfs':

        src_specific_conf = ['--hoodie-conf',
                             'hoodie.deltastreamer.source.dfs.root=' + runtime_step_info['source_path'],
                             '--source-class', runtime_step_info['source_class']]

        if runtime_step_info['source_data_type'] == 'csv':
            src_specific_conf.extend(
                [
                    '--hoodie-conf',
                    'hoodie.deltastreamer.schemaprovider.source.schema.file=' + runtime_step_info['source_schema_path'],
                    '--hoodie-conf',
                    'hoodie.deltastreamer.schemaprovider.target.schema.file=' + runtime_step_info['target_schema_path'],
                    '--hoodie-conf',
                    'hoodie.deltastreamer.csv.header=true',
                    '--hoodie-conf',
                    'hoodie.deltastreamer.csv.sep=' + runtime_step_info['table-extra-parameters'].get("csv-seprator",
                                                                                                      ",")
                ])

    return src_specific_conf


def set_spark_job_configurations(step_details, spark_submit_step):
    hudi_configs = ['--op', step_details['op'],
                        '--hoodie-conf', 'hoodie.datasource.hive_sync.jdbcurl=' + step_details['hive_jdbc_url'],
                        '--props', step_details['property_file_path'],
                        '--hoodie-conf', 'hoodie.datasource.hive_sync.database=' + step_details['glue_db_name'],
                        '--target-base-path', step_details['target_path'],
                        '--target-table', step_details['tbl_name'],
                        '--transformer-class', 'org.apache.hudi.utilities.transform.SqlQueryBasedTransformer',
                        '--enable-sync',
                        '--sync-tool-classes', 'org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool',
                        ]

    hudi_src_data_config = get_source_specific_conf(step_details)

    spark_config = [
        '--conf', 'spark.app.name=' + step_details['step_name'],
        '--conf', 'spark.shuffle.service.enabled=true',
        '--conf', 'spark.dynamicAllocation.enabled=true',
        '--conf', 'spark.dynamicAllocation.initialExecutors=1',
        '--conf', 'spark.dynamicAllocation.cachedExecutorIdleTimeout=25s',
        '--conf', 'spark.dynamicAllocation.executorIdleTimeout=5s',
        '--conf', 'spark.dynamicAllocation.schedulerBacklogTimeout=3s',
        '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
        '--conf', 'spark.sql.hive.convertMetastoreParquet=false',
        '--conf', 'yarn.nodemanager.vmem-check-enabled=false',
        '--conf', 'yarn.nodemanager.pmem-check-enabled=false',
        '--conf', 'spark.kryoserializer.buffer.max=512m',
        '--conf', 'spark.driver.memory=2g',
        '--conf', 'spark.driver.memoryOverhead=512',
        '--conf', 'spark.executor.memory=3g',
        '--conf', 'spark.executor.memoryOverhead=512',
        '--conf', 'spark.executor.cores=1',
        '--conf', 'spark.task.maxFailures=3',
        '--conf', 'spark.yarn.am.attemptFailuresValidityInterval=1h'
    ]
    spark_submit_step['hudi_configs'] = hudi_configs
    spark_submit_step['spark_config'] = spark_config
    spark_submit_step['hudi_src_data_config'] = hudi_src_data_config


def update_step_status_with_stepid(steps_status, step_response):
    step_ids = step_response['StepIds']
    index = 0
    for step in steps_status:
        if step['status'] == 'submitted':
            step['status'] = 'success'
            step['step_id'] = step_ids[index]
            index += 1


def fetch_cluster_id(name):
    log.info("Retrieving cluster id for {0}".format(name))
    cluster_iterator = emr.get_paginator('list_clusters').paginate(
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )
    for page in cluster_iterator:
        for cluster in page['Clusters']:
            if cluster['Name'].lower() == name.lower():
                return cluster['Id']

    return None

# lambda_handler(
#     {
#         "jobs": [
#             {
#                 "job_file_name": "RESOURCES/JOB_SUBMIT/job-submit.json",
#                 "cluster": "datalake-ingestion"
#             }
#         ]      
#     }
#     , None)
