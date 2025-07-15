package org.cameron.cs.common.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.jdk.CollectionConverters._

class SparkManualOffsetsManager(spark: SparkSession) extends Logging {

  /**
   * Retrieves the latest Kafka offsets from the provided DataFrame.
   *
   * @param offsetsDF          DataFrame containing Kafka offsets.
   * @param hdfsOffsetsPath    Path to the HDFS directory where the offsets are stored.
   * @param execDate           Execution date.
   * @param kafkaHost          Kafka bootstrap servers.
   * @param kafkaConsumerGroup Kafka consumer group ID.
   * @param topicName          Kafka topic name.
   * @return JSON string representing the latest offsets for Kafka topics.
   * @throws RuntimeException if the offsetsDF is empty.
   *
   *
   * - kafkapartition  The Kafka partition of the the Kafka topic
   * - latestoffset    The latest offset of the Kafka topic
   * - topic           The name of the Kafka topic associated with the offsets.
   * - consumergroup   The consumer group associated with the Kafka offsets.
   * - execdate        The execution date metadata value.
   * - exectime        The execution time metadata value.
   */
  private def getOffsets(offsetsDF: DataFrame)
                        (hdfsOffsetsPath: String,
                         execDate: String,
                         kafkaHost: String,
                         kafkaConsumerGroup: String,
                         topicName: String): String = {
    if (offsetsDF.isEmpty)
      throw new RuntimeException(s"Offsets dataframe is empty [execdate: $execDate, hdfsOffsetsPath: $hdfsOffsetsPath]")
    else
      try {
        val offsetsData =
          offsetsDF
            .collect()
            .toList
            .map { row =>
              val partition = row.getAs[Int]("kafkapartition")
              val offset = row.getAs[Long]("latestoffset") + 1
              val execDate = row.getAs[String]("execdate")
              val execTime = row.getAs[String]("exectime")
              (partition, offset, execDate, execTime)
            }

        val latestExec = offsetsData.maxBy { case (_, _, execDate, execTime) => (execDate, execTime) }
        val (latestExecDate, latestExecTime) = (latestExec._3, latestExec._4)

        val latestOffsets =
          offsetsData
            .filter { case (_, _, execDate, execTime) => execDate == latestExecDate && execTime == latestExecTime }
            .map { case (partition, offset, _, _) => (partition, offset) }
            .toMap

        val props = new Properties()
        props.put("bootstrap.servers", kafkaHost)
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("group.id", kafkaConsumerGroup)

        val consumer = new KafkaConsumer[String, String](props)
        val topicPartitions = consumer.partitionsFor(topicName).asScala.map(_.partition()).toList
        consumer.close()

        val defaultOffset = "-1" // use "-1" for the latest
        val completeOffsets = topicPartitions.map { partition =>
          val offset = latestOffsets.getOrElse(partition, defaultOffset)
          s""""$partition": $offset"""
        }.mkString(", ")

        s"""{"$topicName": {$completeOffsets}}"""
      } catch {
        case t: Throwable => throw t
      }
  }

  /**
   * Retrieves the latest Kafka offsets from the specified HDFS path and converts them into a JSON string.
   *
   * @param hdfsOffsetsPath    Path to the HDFS directory where the offsets are stored.
   * @param execDate           Execution date.
   * @param kafkaHost          Kafka bootstrap servers.
   * @param kafkaConsumerGroup Kafka consumer group ID.
   * @param topicName          Kafka topic name.
   * @return JSON string representing the latest offsets for Kafka topics.
   */
  def getLatestOffsetsAsStr(hdfsOffsetsPath: String,
                            execDate: String,
                            kafkaHost: String,
                            kafkaConsumerGroup: String,
                            topicName: String): String = {
    logInfo(s"Trying to read the offsets parquet file... [path=$hdfsOffsetsPath]")
    try {
      val offsets = spark.read.parquet(hdfsOffsetsPath)
      getOffsets(offsets)(hdfsOffsetsPath, execDate, kafkaHost, kafkaConsumerGroup, topicName)
    } catch {
      case t: Throwable => throw t
    }
  }
}
