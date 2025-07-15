package org.cameron.cs

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ProcessorUtils._
import org.cameron.cs.common.KeyGenerator
import org.cameron.cs.common.kafka.{SparkKafkaOffsetsWriter, SparkManualOffsetsManager}

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._

class PostsStreamProcessor(spark: SparkSession, conf: PostsStreamConfig) extends Logging {

  import spark.implicits._

  /**
   * Reads a stream of posts from a specified Kafka topic using provided Kafka settings.
   * This function sets up a DataFrame to consume data from the beginning of the topic until the latest message.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param postsTopicName     The name of the Kafka topic from which to read posts.
   * @return DataFrame representing the stream of posts from Kafka.
   */
  private def readPostsStream(kafkaHost: String,
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
      .option("auto.offset.reset", "earliest")
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * Reads a stream of posts from a Kafka topic starting from specified offsets.
   * This function is used to continue reading from a specific point in the Kafka topic, based on previously saved offsets.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param postsTopicName     The name of the Kafka topic from which to read posts.
   * @param startingOffsets    The starting offsets as a JSON string for each partition of the Kafka topic.
   * @return DataFrame representing the stream of posts from Kafka starting from the given offsets.
   */
  private def readPostsStreamWithOffsets(kafkaHost: String,
                                         kafkaConsumerGroup: String,
                                         postsTopicName: String,
                                         startingOffsets: String): DataFrame =
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("group.id", kafkaConsumerGroup)
      .option("subscribe", postsTopicName)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

  def readPostsStreamData(): (DataFrame, Boolean) = {
    var isFirstRun = true

    val postsStream = try {
      val offsetsParams =
        new SparkManualOffsetsManager(spark)
          .getLatestOffsetsAsStr(conf.hdfsOffsetsPath, conf.execDate, conf.kafkaHost, conf.kafkaConsumerGroup, conf.postsTopicName)

      isFirstRun = false
      readPostsStreamWithOffsets(conf.kafkaHost, conf.kafkaConsumerGroup, conf.postsTopicName, s"""{"${conf.postsTopicName}": {$offsetsParams}}""")
    } catch {
      case t: Throwable =>
        logWarning(s"Something went wrong while reading the metrics offset parquet file...", t)
        logInfo(s"Reading the posts stream in Kafka [hosts=${conf.kafkaHost}, topic=${conf.postsTopicName}]")
        readPostsStream(conf.kafkaHost, conf.kafkaConsumerGroup, conf.postsTopicName)
    }

    (postsStream, isFirstRun)
  }

  def transformPosts(flattenedPosts: DataFrame, datePattern: String): DataFrame =
      flattenedPosts
        .withColumn("PartDate", $"CreateDate" cast TimestampType)
        .withColumn("PartDate", to_date($"PartDate", datePattern))
        .withColumn("PostCreateDate", $"CreateDate")

  def transformPrimaries(postsStructsFlattened: DataFrame, primaryCols: Set[String]): DataFrame =
    postsStructsFlattened.select(primaryCols.toSeq.map(col): _*)

  def transformOthers(postsBlogsAuthorsFlattened: DataFrame,
                      excludeJson: JsonNode,
                      primaryCols: Set[String],
                      topLevelExcludes: Set[String]): DataFrame = {
    val nestedAuthorExcludes = excludeJson.get("Author").elements().asScala.map(_.asText()).map("Author" + _).toSet
    val nestedBlogExcludes = excludeJson.get("Blog").elements().asScala.map(_.asText()).map("Blog" + _).toSet
    val nestedExcludes = nestedAuthorExcludes ++ nestedBlogExcludes

    val allExcludesForOthers = nestedExcludes ++ topLevelExcludes ++ primaryCols
    dropFields(postsBlogsAuthorsFlattened.drop(topLevelExcludes.toSeq: _*), allExcludesForOthers.toSeq)
  }

  def transformAttachments(postsAttachments: DataFrame, excludeJson: JsonNode): DataFrame = {
    val attachmentsFieldsToDrop = excludeJson.get("Attachments").elements.asScala.map(_.asText()).toSeq

    val attachmentElementSchema = postsAttachments.schema("Attachments").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val attachmentFields = getNestedFields(attachmentElementSchema)
    val attachmentsExpr =
      s"""
         |transform(Attachments, x -> struct(
         |  ${generateExpr(attachmentFields, attachmentsFieldsToDrop)}
         |))
    """.stripMargin

    // apply the transformation
    val attachmentsRawResult = postsAttachments.select(
      col("Id"),
      col("PostCreateDate"),
      col("PartDate"),
      expr(attachmentsExpr).as("Attachments")
    )
    flattenStruct(attachmentsRawResult.withColumn("Attachment", explode_outer(col("Attachments"))), "Attachment")
  }

  def transformGeoData(postsGeoData: DataFrame): DataFrame = {
    val postGeoDataExploded = postsGeoData.withColumn("GeoData", explode_outer(col("GeoData_arr")))
    flattenStruct(postGeoDataExploded, "GeoData")
  }

  def transformModificationsInfo(postsModificationsInfo: DataFrame): DataFrame =
    flattenStruct(postsModificationsInfo, "ModificationInfo")

  def transformByPartsFlatRawPosts(flattenedFilteredPosts: DataFrame,
                                   execDate: String,
                                   dateTimePath: String,
                                   urlHashUdf: UserDefinedFunction): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val excludeJson: JsonNode = mapper.readTree(conf.excludes)

    val postsBlogsAuthorsFlattened = flattenStructsEx(flattenedFilteredPosts, Seq("Author", "Blog"))

    val postPrimaryColumns: Set[String] = conf.primary
          .split(",")
          .map(_.trim.replace(".", ""))
          .toSet

    val topLevelExcludes: Set[String] = excludeJson.fieldNames.asScala.toSet -- Set("Author", "Blog")

    // use detected struct columns
    val structColsToFlatten = getStructColumns(flattenedFilteredPosts.drop(topLevelExcludes.toSeq: _*))

    val postsStructsFlattened =
      dropDuplicateColumns(flattenStructsEx(flattenedFilteredPosts, structColsToFlatten))

    val allCols: Set[String] = postsStructsFlattened.columns.toSet
    val primaryCols: Set[String] = allCols.intersect(postPrimaryColumns)

    val postsAttachments =
        flattenedFilteredPosts.select($"Id", $"CreateDate".as("PostCreateDate"), $"PartDate", $"Attachments")

    val postsGeoData =
      flattenedFilteredPosts.select($"Id", $"CreateDate".as("PostCreateDate"), $"PartDate", $"GeoData" as "GeoData_arr")

    val postsModificationInfo =
      flattenedFilteredPosts.select($"Id", $"CreateDate".as("PostCreateDate"), $"PartDate", $"ModificationInfo")

    val primaryProcessed = transformPrimaries(postsStructsFlattened, primaryCols = primaryCols)
    val othersProcessed = transformOthers(postsBlogsAuthorsFlattened, excludeJson, primaryCols, topLevelExcludes)
    val attachmentsProcessed = transformAttachments(postsAttachments, excludeJson)
    val geoDataProcessed = transformGeoData(postsGeoData)
    val modificationInfoProcessed = transformModificationsInfo(postsModificationInfo)

    logInfo(s"Writing the new posts [Primary] data...")
    primaryProcessed
      .withColumn("Id", urlHashUdf($"Url"))
      .withColumn("ExecDate", lit(execDate))
      .write
      .mode("overwrite")
      .parquet(s"${conf.hdfsPath}/Posts/delta/parquet/$dateTimePath")

    logInfo(s"Writing the new posts [Other] data...")
    othersProcessed
      .withColumn("ExecDate", lit(execDate))
      .write
      .mode("overwrite")
      .parquet(s"${conf.hdfsPath}/PostsOther/delta/parquet/$dateTimePath")

    logInfo(s"Writing the new posts [Attachments] data...")
    attachmentsProcessed
      .withColumn("ExecDate", lit(execDate))
      .write
      .mode("overwrite")
      .parquet(s"${conf.hdfsPath}/PostsAttachments/delta/parquet/$dateTimePath")

    logInfo(s"Writing the new posts [GeoData] data...")
    geoDataProcessed
      .withColumn("ExecDate", lit(execDate))
      .write
      .mode("overwrite")
      .parquet(s"${conf.hdfsPath}/PostsGeoData/delta/parquet/$dateTimePath")

    logInfo(s"Writing the new posts [ModificationInfo] data...")
    modificationInfoProcessed
      .withColumn("ExecDate", lit(execDate))
      .write
      .mode("overwrite")
      .parquet(s"${conf.hdfsPath}/PostsModificationInfo/delta/parquet/$dateTimePath")
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
   *
   * Processes the stream of posts from Kafka and writes the transformed data to HDFS.
   * This method manages the entire data pipeline - reading from Kafka, transforming data, and writing to HDFS.
   * It also handles offset management for Kafka to ensure exactly-once semantics.
   */
  def process(): Unit = {
    val execDate: String           = conf.execDate
    val kafkaConsumerGroup: String = conf.kafkaConsumerGroup
    val postsTopicName: String     = conf.postsTopicName
    val hdfsOffsetsPath: String    = conf.hdfsOffsetsPath
    val datePattern = "yyyy-MM-dd"

    val (postsStream, isFirstRun) = readPostsStreamData()
    val postsSchema = spark.read.json((postsStream select $"value").as[String]).schema

    val postsData =
      postsStream
        .withColumn("jsonData", from_json($"value" cast StringType, postsSchema))
        .withColumn("CurrentTimestamp", current_timestamp())

    val urlHash: UserDefinedFunction = udf { url: String => KeyGenerator.getKey(url) }

    val flattenedPosts =
      postsData.select($"jsonData.*", $"CurrentTimestamp", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("Id", urlHash($"Url"))

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"

    logInfo(s"Writing the raw posts [PostsRaw] data...")
    dropDuplicateColumns(
      transformPosts(flattenedPosts, datePattern)
    ).withColumn("Id", urlHash($"Url"))
      .write
      .mode("overwrite")
      .parquet(s"{${conf.hdfsPath}/PostsRaw/delta/parquet/$dateTimePath")

    val flattenedFilteredPosts = spark.read.parquet(s"${conf.hdfsPath}/PostsRaw/delta/parquet/$dateTimePath").cache()

    transformByPartsFlatRawPosts(flattenedFilteredPosts, execDate, dateTimePath, urlHash)

    new SparkKafkaOffsetsWriter(spark)
      .saveOffsetsWithExecDate(flattenedFilteredPosts)(isFirstRun, postsTopicName, kafkaConsumerGroup, execDate, curTime, hdfsOffsetsPath)

    flattenedFilteredPosts.unpersist()

    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    logInfo(s"Removing the raw posts data [$dateTimePath]...")
    hdfs.delete(new Path(s"${conf.hdfsPath}/PostsRaw/delta/parquet/$dateTimePath"), true)

    hdfs.close()
  }
}
