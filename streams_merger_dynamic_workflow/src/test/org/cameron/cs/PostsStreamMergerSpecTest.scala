package org.cameron.cs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.cameron.cs.common.TestUtils

import java.time.LocalDate

trait PostsStreamMergerSpecTest extends MetricsStreamTest {

  override lazy val spark: SparkSession = TestUtils.sharedSparkSession("Posts Stream Merger Test")
  lazy val currentDay = LocalDate.now()

  lazy val postStreamsMergerTestConfig: StreamsMergerConfig =
    StreamsMergerConfig(
      execDate = currentDay.toString,
      prevExecDate = currentDay.minusDays(1).toString,
      lowerBound = currentDay.minusDays(30).toString,
      postsPath = "data/posts_stream_job/Posts/delta/parquet",
      postsOffsetsPath = "data/posts_stream_job/offsets/Posts/parquet",
      metricsPath = "data/metrics_stream_job/Metrics/delta/parquet",
      metricsOffsetsPath = "data/metrics_stream_job/offsets/Metrics/parquet",
      blogsPath = "data/blogs_stream_job/Blogs/delta/parquet",
      blogsOffsetsPath = "data/blogs_stream_job/offsets/Blogs/parquet",
      mergedBlogsPath = "data/Blogs/parquet",
      mergedPostsPath = "data/Posts/parquet",
      skipTrash = "true",
      batchSize = "10")

  lazy val postsStreamTestConfig: PostsStreamConfig =
    PostsStreamConfig(
      execDate = postStreamsMergerTestConfig.execDate,
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
    super.beforeAll()
    generatePostsSyntheticData(postsStreamTestConfig.postsTopicName, 30)
  }
}