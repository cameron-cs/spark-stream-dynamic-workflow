package org.cameron.cs

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{current_timestamp, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cameron.cs.common.kafka.{SparkKafkaOffsetsWriter, SparkManualOffsetsManager}

import scala.jdk.CollectionConverters._
import java.time.LocalTime
import java.time.format.DateTimeFormatter

class BlogsStreamProcessor(spark: SparkSession, conf: BlogsStreamConfig) extends Logging {

  import spark.implicits._

  /**
   * Reads a stream of blog data from a Kafka topic starting from specified offsets.
   * This function is essential for resuming data processing from specific points in the Kafka topic, based on previously saved offsets.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param blogsTopicName     The name of the Kafka topic from which to read blog data.
   * @param startingOffsets    The starting offsets as a JSON string for each partition of the Kafka topic.
   * @return DataFrame representing the stream of blog data from Kafka starting from the given offsets.
   */
  private def readBlogsStreamWithOffsets(kafkaHost: String,
                                         kafkaConsumerGroup: String,
                                         blogsTopicName: String,
                                         startingOffsets: String): DataFrame =
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("group.id", kafkaConsumerGroup)
      .option("subscribe", blogsTopicName)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

  /**
   * Initiates a DataFrame to read a stream of blog data from a specified Kafka topic.
   * This function sets up the stream to consume data from the earliest available message in the topic to the latest.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param blogsTopicName     The name of the Kafka topic from which to read blog data.
   * @return DataFrame representing the stream of blog data from Kafka.
   */
  private def readBlogsStream(kafkaHost: String,
                              kafkaConsumerGroup: String,
                              blogsTopicName: String): DataFrame =
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("group.id", kafkaConsumerGroup)
      .option("subscribe", blogsTopicName)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

  def readBlogsStreamData(): (DataFrame, Boolean) = {
    var isFirstRun = true

    val blogsStream = try {
        val offsetsParams =
          new SparkManualOffsetsManager(spark)
            .getLatestOffsetsAsStr(conf.hdfsOffsetsPath, conf.execDate, conf.kafkaHost, conf.kafkaConsumerGroup, conf.blogsTopicName)

        isFirstRun = false
        readBlogsStreamWithOffsets(conf.kafkaHost, conf.kafkaConsumerGroup, conf.blogsTopicName, s"""{"${conf.blogsTopicName}": {$offsetsParams}}""")
      } catch {
        case t: Throwable =>
          logWarning(s"Something went wrong while reading the blogs offset parquet file...", t)
          logInfo(s"Reading the blogs stream in Kafka [hosts=${conf.kafkaHost}, topic=${conf.blogsTopicName}]")
          readBlogsStream(conf.kafkaHost, conf.kafkaConsumerGroup, conf.blogsTopicName)
      }

    (blogsStream, isFirstRun)
  }

  def transformBlogs(flattenedBlog: DataFrame,
                     blogsExclude: String,
                     blogsPrimary: String): DataFrame = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val blogExcludeJson = mapper.readTree(blogsExclude)

    val blogsExcludeSet: Set[String] = blogExcludeJson.fieldNames().asScala.toSet

    val blogPrimaryColumns: Seq[String] = blogsPrimary.split(",").map(_.trim).toIndexedSeq

    BlogsStreamUtils
        .processBlogsExclude(flattenedBlog, blogsExcludeSet) // replace it with BlogsStreamUtils.processBlogs(flattenedBlogs, blogPrimaryColumns, blogsExcludeSet)
        .withColumn("CurrentTimestamp", current_timestamp())
  }

  /**
   * Main processing function that orchestrates the data flow from Kafka to HDFS.
   * This function encompasses several steps: reading data from Kafka, either from the earliest offset or a specific checkpoint;
   * transforming the data as per the schema; and finally, writing the transformed data to HDFS.
   * It also manages Kafka offsets to ensure data consistency and fault tolerance.
   *
   * The process begins by attempting to read the latest saved offsets from HDFS. If this read is successful,
   * the function resumes reading from Kafka using these offsets, ensuring continuity of data processing.
   * In case of any issues (such as missing offset information), the function defaults to reading from the earliest available data in Kafka.
   *
   * Once the data is read from Kafka, it undergoes transformation. The raw Kafka message is parsed to extract relevant fields.
   * Additional fields are conditionally added based on the existence of nested columns within the data.
   * This step ensures the DataFrame matches the expected schema, adding nulls where data is not available.
   *
   * The final step involves writing the transformed data to HDFS. The location and format are defined in the configuration.
   * Additionally, the latest Kafka offsets are calculated and stored in HDFS. This step is crucial for ensuring that subsequent
   * runs of the processor start from the correct position in the Kafka topic.
   *
   * @note This function uses a combination of Spark SQL operations and Kafka's consumer API to achieve its objectives.
   *       It is designed to handle large streams of data efficiently while ensuring data consistency and fault tolerance.
   */
  def process(): Unit = {
    val execDate: String           = conf.execDate
    val kafkaConsumerGroup: String = conf.kafkaConsumerGroup
    val blogsTopicName: String     = conf.blogsTopicName
    val hdfsPath: String           = conf.hdfsPath
    val hdfsOffsetsPath: String    = conf.hdfsOffsetsPath
    val blogsExclude: String       = conf.excludes
    val blogsPrimary: String       = conf.primary

    val (blogsStream, isFirstRun) = readBlogsStreamData()

    val schema = spark.read.json((blogsStream select $"value").as[String]).schema

    val blogsData = blogsStream
      .withColumn("jsonData", from_json($"value" cast StringType, schema))

    val flattenedBlogs = blogsData.select($"jsonData.*", $"partition" as "KafkaPartition", $"offset" as "KafkaOffset")

    val blogsFinal: DataFrame = transformBlogs(flattenedBlogs, blogsExclude, blogsPrimary)

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"

    new SparkKafkaOffsetsWriter(spark)
      .saveOffsetsWithExecDate(blogsFinal)(isFirstRun, blogsTopicName, kafkaConsumerGroup, execDate, curTime, hdfsOffsetsPath, "KafkaPartition", "KafkaOffset")

    logInfo(s"Writing the blogs data...")

    blogsFinal
      .write
      .mode("overwrite")
      .parquet(s"$hdfsPath/$dateTimePath")
  }
}
