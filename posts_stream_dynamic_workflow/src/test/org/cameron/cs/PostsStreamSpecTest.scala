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

trait PostsStreamSpecTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = TestUtils.sharedSparkSession("Posts Stream Test")

  lazy val postsTestStreamConfig: PostsStreamConfig =
    PostsStreamConfig(
      execDate = LocalDate.now().toString,
      kafkaHost = "localhost:9092",
      kafkaConsumerGroup = "Vanilla_Hadoop_Test_Posts_stream_to_parquet_1",
      postsTopicName = "PostsTest",
      hdfsPath = "data/posts_stream_job",
      hdfsOffsetsPath = "data/posts_stream_job/Posts/offsets/Posts/parquet",
      excludes = PostsTestDataStreamSchemaUtils.excludes,
      primary = PostsTestDataStreamSchemaUtils.primary)

  private def generatePostsSyntheticData(postsTopicName: String, count: Int): Unit = {
    val emptyDF = TestUtils.generateRowsWithDefaults(spark, PostsTestDataStreamSchemaUtils.postsTestSchema, count)
      .withColumn("Url", lit("https://example.com/id_user_test_1"))

    val jsonStrings: Seq[String] = emptyDF.toJSON.collect().toSeq
    TestUtils.sendToKafkaRecords(jsonStrings, postsTopicName)
  }

  override def beforeAll(): Unit = {
    TestUtils.dockerComposeUp()
    generatePostsSyntheticData(postsTestStreamConfig.postsTopicName, 30)
  }

  override def afterAll(): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, postsTestStreamConfig.kafkaHost)
    val adminClient = AdminClient.create(props)

    try {
      val deleteTopicsResult: DeleteTopicsResult = adminClient.deleteTopics(List(postsTestStreamConfig.postsTopicName).asJava)
      deleteTopicsResult.all().get(10, java.util.concurrent.TimeUnit.SECONDS)

      println(s"[INFO] Topic '${postsTestStreamConfig.postsTopicName}' deleted successfully.")
    } catch {
      case e: Exception =>
        println(s"[ERROR] Failed to delete topic '${postsTestStreamConfig.postsTopicName}': ${e.getMessage}")
    } finally {
      adminClient.close()
    }
    TestUtils.dockerComposeDown()
  }
}
