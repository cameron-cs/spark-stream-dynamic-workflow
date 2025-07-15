package org.cameron.cs

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DeleteTopicsResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.cameron.cs.common.TestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate
import java.util.Properties
import scala.jdk.CollectionConverters._

trait MetricsStreamSpecTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = TestUtils.sharedSparkSession("Metrics Stream Test")

  lazy val metricsTestStreamConfig: MetricsStreamConfig =
    MetricsStreamConfig(
      execDate = LocalDate.now().toString,
      kafkaHost = "localhost:9092",
      kafkaConsumerGroup = "Vanilla_Hadoop_Test_Metrics_stream_to_parquet_1",
      metricsTopicName = "MetricsTest",
      hdfsPath = "data/metrics_stream_job/Metrics/delta/parquet",
      hdfsOffsetsPath = "data/metrics_stream_job/Metrics/offsets/Metrics/parquet")

  private def generateMetricsSyntheticData(metricsTopicName: String, count: Int): Unit = {
    val emptyDF = TestUtils.generateRowsWithDefaults(spark, MetricsTestDataStreamSchemaUtil.metricsTestSchema, count)
      .withColumn("Url", lit("https://example.com/id_user_test_1"))

    val jsonStrings: Seq[String] = emptyDF.toJSON.collect().toSeq
    TestUtils.sendToKafkaRecords(jsonStrings, metricsTopicName)
  }

  override def beforeAll(): Unit = {
    TestUtils.dockerComposeUp()
    generateMetricsSyntheticData(metricsTestStreamConfig.metricsTopicName, 10)
  }

  override def afterAll(): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, metricsTestStreamConfig.kafkaHost)
    val adminClient = AdminClient.create(props)

    try {
      val deleteTopicsResult: DeleteTopicsResult = adminClient.deleteTopics(List(metricsTestStreamConfig.metricsTopicName).asJava)
      deleteTopicsResult.all().get(10, java.util.concurrent.TimeUnit.SECONDS)

      println(s"[INFO] Topic '${metricsTestStreamConfig.metricsTopicName}' deleted successfully.")
    } catch {
      case e: Exception =>
        println(s"[ERROR] Failed to delete topic '${metricsTestStreamConfig.metricsTopicName}': ${e.getMessage}")
    } finally {
      adminClient.close()
    }

    TestUtils.dockerComposeDown()
  }
}
