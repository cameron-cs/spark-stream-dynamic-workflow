package org.cameron.cs

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DeleteTopicsResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.cameron.cs.common.TestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate
import java.util.Properties
import scala.jdk.CollectionConverters._

trait BlogsStreamSpecTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = TestUtils.sharedSparkSession("Blog Stream Test")

  lazy val blogsTestStreamConfig: BlogsStreamConfig =
    BlogsStreamConfig(
      execDate = LocalDate.now().toString,
      kafkaHost = "localhost:9092",
      kafkaConsumerGroup = "Vanilla_Hadoop_Test_Blogs_stream_to_parquet_1",
      blogsTopicName = "BlogsTest",
      hdfsPath = "data/blogs_stream_job/Blogs/delta/parquet",
      hdfsOffsetsPath = "data/blogs_stream_job/offsets/Blogs/parquet",
      excludes = BlogsTestDataStreamSchemaUtil.excludes,
      primary = BlogsTestDataStreamSchemaUtil.primary)

  def generateBlogsSyntheticData(blogsTopicName: String, count: Int): Unit = {
    val urlHash = udf { url: String => TestUtils.dummyHashString(url) }

    val emptyDF = TestUtils.generateRowsWithDefaults(spark, BlogsTestDataStreamSchemaUtil.blogsTestSchema, count)
      .withColumn("Url", lit("https://example.com/id_user_test"))
      .withColumn("UrlHash", urlHash(col("Url")))

    val jsonStrings: Seq[String] = emptyDF.toJSON.collect().toSeq

    TestUtils.sendToKafkaRecords(jsonStrings, blogsTopicName)
  }

  override def beforeAll(): Unit = {
    TestUtils.dockerComposeUp()
    generateBlogsSyntheticData(blogsTestStreamConfig.blogsTopicName, 100)
  }

  override def afterAll(): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, blogsTestStreamConfig.kafkaHost)
    val adminClient = AdminClient.create(props)

    try {
      val deleteTopicsResult: DeleteTopicsResult = adminClient.deleteTopics(List(blogsTestStreamConfig.blogsTopicName).asJava)
      deleteTopicsResult.all().get(10, java.util.concurrent.TimeUnit.SECONDS)

      println(s"[INFO] Topic '${blogsTestStreamConfig.blogsTopicName}' deleted successfully.")
    } catch {
      case e: Exception =>
        println(s"[ERROR] Failed to delete topic '${blogsTestStreamConfig.blogsTopicName}': ${e.getMessage}")
    } finally {
      adminClient.close()
    }

    TestUtils.dockerComposeDown()
  }
}
