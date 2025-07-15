package org.cameron.cs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{current_timestamp, from_json, lit, max, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cameron.cs.common.KeyGenerator
import org.cameron.cs.common.kafka.{SparkKafkaOffsetsWriter, SparkManualOffsetsManager}

import java.time.LocalTime
import java.time.format.DateTimeFormatter

class MetricsStreamProcessor(spark: SparkSession, conf: MetricsStreamConfig) extends Logging {

  import spark.implicits._

  /**
   * Reads a stream of metrics data from a Kafka topic starting from specified offsets.
   * This function is essential for resuming data processing from a specific point in the Kafka topic, based on previously saved offsets.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param metricsTopicName   The name of the Kafka topic from which to read metrics data.
   * @param startingOffsets    The starting offsets as a JSON string for each partition of the Kafka topic.
   * @return DataFrame representing the stream of metrics data from Kafka starting from the given offsets.
   */
  private def readMetricsStreamWithOffsets(kafkaHost: String,
                                           kafkaConsumerGroup: String,
                                           metricsTopicName: String,
                                           startingOffsets: String): DataFrame =
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("group.id", kafkaConsumerGroup)
      .option("subscribe", metricsTopicName)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

  /**
   * Initiates a DataFrame to read a stream of metrics data from a specified Kafka topic.
   * This function sets up the stream to consume data from the earliest available message in the topic to the latest.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param metricsTopicName   The name of the Kafka topic from which to read metrics data.
   * @return DataFrame representing the stream of metrics data from Kafka.
   */
  def readMetricsStream(kafkaHost: String,
                        kafkaConsumerGroup: String,
                        postsTopicName: String): DataFrame = {
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", postsTopicName)
      .option("group.id", kafkaConsumerGroup)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
  }

  def readMetricsStreamData(): (DataFrame, Boolean) = {
    var isFirstRun = true

    val metricsStream = try {
      val offsetsParams =
        new SparkManualOffsetsManager(spark)
          .getLatestOffsetsAsStr(conf.hdfsOffsetsPath, conf.execDate, conf.kafkaHost, conf.kafkaConsumerGroup, conf.metricsTopicName)

      isFirstRun = false
      readMetricsStreamWithOffsets(conf.kafkaHost, conf.kafkaConsumerGroup, conf.metricsTopicName, s"""{"${conf.metricsTopicName}": {$offsetsParams}}""")
    } catch {
      case t: Throwable =>
        logWarning(s"Something went wrong while reading the metrics offset parquet file...", t)
        logInfo(s"Reading the metrics stream in Kafka [hosts=${conf.kafkaHost}, topic=${conf.metricsTopicName}]")
        readMetricsStream(conf.kafkaHost, conf.kafkaConsumerGroup, conf.metricsTopicName)
    }

    (metricsStream, isFirstRun)
  }

  def transformMetrics(flattenedMetrics: DataFrame): DataFrame =
    flattenedMetrics.select(
      $"Blog.Type" as "blogtype",
      $"Blog.Url" as "blogurl",
      $"CreateDate" as "partdate",
      $"Metrics.CommentCount" as "comments",
      $"Metrics.LikeCount" as "likes",
      $"Metrics.RepostCount" as "reposts",
      $"Metrics.ViewCount" as "views",
      $"Metrics.CommentNegativeCount" as "commentnegativecount",
      $"Metrics.CommentPositiveCount" as "commentpositivecount",
      $"Metrics.Timestamp" as "metricstimestamp",
      $"Type" as "metricstype",
      $"Url" as "metricsurl",
      $"CurrentTimestamp" as "currenttimestamp",
      $"kafkapartition" as "kafkapartition",
      $"kafkaoffset" as "kafkaoffset"
    )

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
   *
   * Processes the stream of metrics data from Kafka and writes the transformed data to HDFS.
   * This method manages the complete data processing pipeline: reading from Kafka, data transformation, and writing to HDFS.
   * It also handles offset management to ensure exactly-once processing semantics.
   */
  def process(): Unit = {
    val execDate: String           = conf.execDate
    val kafkaConsumerGroup: String = conf.kafkaConsumerGroup
    val metricsTopicName: String   = conf.metricsTopicName
    val hdfsPath: String           = conf.hdfsPath
    val hdfsOffsetsPath: String    = conf.hdfsOffsetsPath

    val (metricsStream, isFirstRun) = readMetricsStreamData()

    val metricsSchema = spark.read.json((metricsStream select $"value").as[String]).schema

    val metricsData = metricsStream
      .withColumn("jsonData", from_json($"value" cast StringType, metricsSchema))
      .withColumn("CurrentTimestamp", current_timestamp())

    val flattenedMetrics = metricsData.select($"jsonData.*", $"CurrentTimestamp", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")

    val urlHash = udf { url: String => KeyGenerator.getKey(url) }

    val metricsFinal =
      transformMetrics(flattenedMetrics)
        .withColumn("id", urlHash($"metricsurl"))
        .withColumn("execdate", lit(execDate))

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"

    new SparkKafkaOffsetsWriter(spark)
      .saveOffsetsWithExecDate(metricsFinal)(isFirstRun, metricsTopicName, kafkaConsumerGroup, execDate, curTime, hdfsOffsetsPath)

    logInfo(s"Writing the metrics data...")

    metricsFinal
      .write
      .mode("overwrite")
      .parquet(s"$hdfsPath/$dateTimePath")
  }
}
