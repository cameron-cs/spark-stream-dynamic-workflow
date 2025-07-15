from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.macros import ds_add

default_args = {
    'owner': 'root',
    'start_date': datetime(2025, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='streams_merger_dynamic_workflow_job_dag',
          default_args=default_args,
          start_date=datetime(2025, 3, 13),
          schedule_interval='0 23 * * *',
          catchup=False
          )

airflow_var = Variable.get("streams_merger_dynamic_workflow_params", deserialize_json=True)

hdfs_posts = airflow_var['hdfsPostsPath']
hdfs_offsets_posts = airflow_var['hdfsOffsetsPostsPath']
hdfs_blogs = airflow_var['hdfsBlogsPath']
hdfs_offsets_blogs = airflow_var['hdfsOffsetsBlogsPath']
hdfs_metrics = airflow_var['hdfsMetricsPath']
hdfs_offsets_metrics = airflow_var['hdfsOffsetsMetricsPath']
hdfs_merged_blogs = airflow_var['hdfsMergedBlogsPath']
hdfs_merged_posts = airflow_var['hdfsMergedPostsPath']
skip_trash = airflow_var['hdfsSkipTrash']
self_only = airflow_var['selfOnly']
edge_name = airflow_var['edgeName']
batch_size = airflow_var['batchSize']
day_frame = airflow_var['dayFrame']

exec_date = ''
prev_exec_date = ''
lower_bound = ''

marker_alias = 'streams_merger_dynamic_workflow_marker'
trigger_dag_alias = 'streams_merger_dynamic_workflow_trigger_dag'


def set_conf_init(**kwargs):
    if 'dag_run' in kwargs and kwargs['dag_run'].conf:
        conf = kwargs['dag_run'].conf
        exec_date = conf.get('exec_date', None)
        prev_exec_date = conf.get('prev_exec_date', None)
        lower_bound = conf.get('lower_bound', None)
        _self_only = conf.get('self_only', False)
    else:
        exec_date = (kwargs['execution_date'] - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_exec_date = kwargs['prev_execution_date'].strftime("%Y-%m-%d")
        lower_bound = datetime.strptime(ds_add(kwargs['ds'], -30), "%Y-%m-%d").strftime("%Y-%m-%d")
        _self_only = self_only

    return {
        'exec_date': exec_date,
        'prev_exec_date': prev_exec_date,
        'lower_bound': lower_bound,
        'self_only': _self_only
    }




def _check_self_only(**context):
    _self_only = True
    if 'dag_run' in context and context['dag_run'].conf:
        conf = context['dag_run'].conf
        _self_only = conf.get('self_only', True)
    return "streams_noop_marker" if _self_only else trigger_dag_alias


start = PythonOperator(
    task_id='start_streams_merger_dynamic_workflow_job',
    python_callable=set_conf_init,
    dag=dag,
    trigger_rule='none_failed'
)


spark_blogs_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_blogs_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.BlogsStreamsMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_blogs_merger_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash)
    ],
    dag=dag
)


spark_posts_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_posts_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsStreamsMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_merger_dynamic_workflow_posts_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.file.buffer": "1m",
        "spark.reducer.maxSizeInFlight": "128m",
        "spark.dynamicAllocation.initialExecutors": "10",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "100",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "6",
        "spark.executor.cores": "4",
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "8g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
    ],
    trigger_rule = 'all_done',
    dag=dag
)


spark_posts_other_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_posts_other_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsOtherMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_posts_other_merger_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.file.buffer": "1m",
        "spark.reducer.maxSizeInFlight": "128m",
        "spark.dynamicAllocation.initialExecutors": "10",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "100",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "6",
        "spark.executor.cores": "4",
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "8g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
    ],
    dag=dag
)

spark_posts_attachments_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_posts_attachments_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsAttachmentsMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_posts_attachments_merger_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.file.buffer": "1m",
        "spark.reducer.maxSizeInFlight": "128m",
        "spark.dynamicAllocation.initialExecutors": "10",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "100",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "6",
        "spark.executor.cores": "4",
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "8g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
    ],
    dag=dag
)

spark_posts_geo_data_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_posts_geo_data_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsGeoDataMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_posts_geo_data_merger_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.file.buffer": "1m",
        "spark.reducer.maxSizeInFlight": "128m",
        "spark.dynamicAllocation.initialExecutors": "10",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "100",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "6",
        "spark.executor.cores": "4",
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "8g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
    ],
    dag=dag
)

spark_posts_modifications_job = SparkSubmitOperator(
    task_id='streams_dynamic_workflow_posts_modifications_merger_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsModificationsMergerApp',
    application='/usr/local/airflow/spark/streams_merger_dynamic_workflow/streams_merger_dynamic_workflow.jar',
    name='streams_posts_modifications_merger_job',
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.shuffle.file.buffer": "1m",
        "spark.reducer.maxSizeInFlight": "128m",
        "spark.dynamicAllocation.initialExecutors": "10",
        "spark.dynamicAllocation.minExecutors": "10",
        "spark.dynamicAllocation.maxExecutors": "100",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "6",
        "spark.executor.cores": "4",
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "8g"
    },
    verbose=False,
    retries=0,
    application_args=[
        '-d', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['exec_date'] }}",
        '--prevExecDate', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['prev_exec_date'] }}",
        '--lowerBound', "{{ task_instance.xcom_pull(task_ids='start_streams_merger_dynamic_workflow_job')['lower_bound'] }}",
        '-p', hdfs_posts,
        '--po', hdfs_offsets_posts,
        '-b', hdfs_blogs,
        '--bo', hdfs_offsets_blogs,
        '-m', hdfs_metrics,
        '--mo', hdfs_offsets_metrics,
        '--mb', hdfs_merged_blogs,
        '--mp', hdfs_merged_posts,
        '--skipTrash', str(skip_trash),
        "--batchSize", str(batch_size)
    ],
    dag=dag
)


marker = PythonOperator(
    task_id=marker_alias,
    python_callable=set_conf_init,
    trigger_rule='none_failed'
)


check_self_only = BranchPythonOperator(
    task_id='check_self_only',
    python_callable=_check_self_only,
    trigger_rule='all_done',
)


trigger_edge_dag = EmptyOperator(task_id=trigger_dag_alias, trigger_rule='none_failed') if edge_name is None else TriggerDagRunOperator(
    task_id=trigger_dag_alias,
    trigger_dag_id='...',
    python_callable=set_conf_init,
)

marker_1 = PythonOperator(task_id='marker_1', python_callable=set_conf_init, trigger_rule='none_failed', dag=dag)
marker_2 = PythonOperator(task_id='marker_2', python_callable=set_conf_init, trigger_rule='none_failed', dag=dag)
marker_3 = PythonOperator(task_id='marker_3', python_callable=set_conf_init, trigger_rule='none_failed', dag=dag)
marker_4 = PythonOperator(task_id='marker_4', python_callable=set_conf_init, trigger_rule='none_failed', dag=dag)
marker_final = PythonOperator(task_id='marker_final', python_callable=set_conf_init, trigger_rule='none_failed', dag=dag)

noop_marker = EmptyOperator(task_id="streams_noop_marker", dag=dag)

join_after_branch = EmptyOperator(
    task_id="join_after_branch",
    dag=dag
)


# === DAG chaining ===
start >> spark_blogs_job >> spark_posts_job
spark_posts_job >> marker_1 >> spark_posts_other_job
spark_posts_other_job >> marker_2 >> spark_posts_attachments_job
spark_posts_attachments_job >> marker_3 >> spark_posts_geo_data_job
spark_posts_geo_data_job >> marker_4 >> spark_posts_modifications_job
spark_posts_modifications_job >> marker_final >> check_self_only

# branch execution paths
check_self_only >> trigger_edge_dag
check_self_only >> noop_marker

# final join (optional, but recommended to avoid dangling branches)
[trigger_edge_dag, noop_marker] >> join_after_branch
