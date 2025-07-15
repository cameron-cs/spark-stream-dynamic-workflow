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

dag = DAG(dag_id='posts_stream_dynamic_workflow_job_dag',
          default_args=default_args,
          start_date=datetime(2025, 3, 13),
          schedule_interval=timedelta(hours=3, minutes=0),
          catchup=False
          )

airflow_var = Variable.get("posts_stream_dynamic_workflow_params", deserialize_json=True)

kafka_host = airflow_var['kafkaHost']
kafka_consumer_group = airflow_var['kafkaConsumerGroup']
posts_topic = airflow_var['postsTopicName']
hdfs_path = airflow_var['hdfsPath']
hdfs_offsets_path = airflow_var['hdfsOffsetsPath']
excludes = airflow_var['excludes']
primary = airflow_var['primary']

spark_job = SparkSubmitOperator(
    task_id='posts_stream_dynamic_workflow_job',
    yarn_queue='airflow',
    java_class = 'org.cameron.cs.PostsStreamApp',
    application='/usr/local/airflow/spark/posts_stream_dynamic_workflow/posts_stream_dynamic_workflow.jar',
    name='posts_stream_dynamic_workflow_job',
    application_args=[
        '-d', '{{ ds }}',
        '-h', kafka_host,
        '-g', kafka_consumer_group,
        '-t', posts_topic,
        '-p', hdfs_path,
        '-o', hdfs_offsets_path,
        '-e', str(excludes),
        '--primary', str(primary)
    ],
    conf={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.hadoop.validateOutputSpecs": "false",
        "spark.yarn.queue": "airflow",
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "15",
        "spark.driver.memory": "16g",
        "spark.driver.cores": "4",
        "spark.executor.memory": "16g",
        "spark.executor.cores": "4",
        "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        "spark.sql.json.maxStringLength": "10000000",
        "spark.sql.shuffle.partitions": "400",
        "spark.streaming.kafka.maxRatePerPartition": "10000",
        "spark.sql.files.maxPartitionBytes": "256MB",
        "spark.kafka.consumer.fetchOffset.bytes": "67108864",
        "spark.kafka.consumer.max.partition.fetch.bytes": "67108864",
        "spark.kafka.consumer.pollTimeoutMs": "60000"
    },
    dag=dag
)

start = DummyOperator(
    task_id='start_posts_stream_dynamic_workflow_job',
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end_posts_stream_dynamic_workflow_job',
    trigger_rule='none_failed'
)

start >> spark_job >> end
