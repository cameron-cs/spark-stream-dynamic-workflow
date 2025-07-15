from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'root',
    'start_date': datetime(2025, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='blogs_stream_dynamic_workflow_job_dag',
          default_args=default_args,
          start_date=datetime(2025, 3, 13),
          schedule_interval='0 * * * *',
          catchup=False
          )

airflow_var = Variable.get("blogs_stream_dynamic_workflow_params", deserialize_json=True)

kafka_host = airflow_var['kafkaHost']
kafka_consumer_group = airflow_var['kafkaConsumerGroup']
blogs_topic = airflow_var['blogsTopicName']
hdfs_path = airflow_var['hdfsPath']
hdfs_offsets_path = airflow_var['hdfsOffsetsPath']
excludes = airflow_var['excludes']
primary = airflow_var['primary']

spark_job = SparkSubmitOperator(
    task_id='blogs_stream_dynamic_workflow_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.BlogsStreamApp',
    application='/usr/local/airflow/spark/blogs_stream_dynamic_workflow/blogs_stream_dynamic_workflow.jar',
    name='blogs_stream_dynamic_workflow_job',
    application_args=[
        '-d', '{{ ds }}',
        '-h', kafka_host,
        '-g', kafka_consumer_group,
        '-t', blogs_topic,
        '-p', hdfs_path,
        '-o', hdfs_offsets_path,
        "--excludes", str(excludes),
        "--primary", primary
    ],
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "20",
        "spark.driver.memory": "8g",
        "spark.driver.cores": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "8g"
    },
    dag=dag
)

start = DummyOperator(
    task_id='start_blogs_stream_dynamic_workflow_job',
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end_blogs_stream_dynamic_workflow_job',
    trigger_rule='none_failed'
)

start >> spark_job >> end
