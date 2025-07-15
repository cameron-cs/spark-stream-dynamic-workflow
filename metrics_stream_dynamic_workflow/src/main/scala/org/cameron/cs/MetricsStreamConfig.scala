package org.cameron.cs

import scopt.{OParser, OParserBuilder}

/**
 * Configuration class for MetricsStreamProcessor.
 * This class encapsulates all necessary configurations required by the MetricsStreamProcessor to read from Kafka and write to HDFS.
 *
 * @param execDate The execution date for the current processing job.
 * @param kafkaHost Kafka host information.
 * @param kafkaConsumerGroup Kafka consumer group ID.
 * @param metricsTopicName Name of the Kafka topic to read metrics data from.
 * @param hdfsPath Path in HDFS where the processed data should be written.
 * @param hdfsOffsetsPath Path in HDFS to store and read offsets.
 */
case class MetricsStreamConfig(execDate: String           = "",
                               kafkaHost: String          = "",
                               kafkaConsumerGroup: String = "",
                               metricsTopicName: String   = "",
                               hdfsPath: String           = "",
                               hdfsOffsetsPath: String    = "")

/**
 * Companion object for MetricsStreamConfig case class.
 * Contains the command-line argument parser for configuring the MetricsStreamProcessor.
 * Utilizes the scopt library to define and parse command-line options, providing a user-friendly interface for setting up the processor.
 */
object MetricsStreamConfig {

  val builder: OParserBuilder[MetricsStreamConfig] = OParser.builder[MetricsStreamConfig]

  val parser: OParser[Unit, MetricsStreamConfig] = {

    import builder._

    OParser.sequence(
      programName("MetricsStreamApp"),
      head("MetricsStreamApp", "0.1"),

      opt[String]('d', "execDate")
        .action((x, c) => c.copy(execDate = x))
        .text("Execution date"),

      opt[String]('h', "kafkaHost")
        .action((x, c) => c.copy(kafkaHost = x))
        .text("Kafka hosts"),

      opt[String]('g', "kafkaConsumerGroup")
        .action((x, c) => c.copy(kafkaConsumerGroup = x))
        .text("Kafka Consumer Group"),

      opt[String]('t', "kafkaMetricsTopicName")
        .action((x, c) => c.copy(metricsTopicName = x))
        .text("Kafka Metrics topic name"),

      opt[String]('p', "hdfsPath")
        .action((x, c) => c.copy(hdfsPath = x))
        .text("Path to HDFS"),

      opt[String] ('o', "hdfsOffsetsPath")
        .action((x, c) => c.copy(hdfsOffsetsPath = x))
        .text("Offsets path to HDFS")
    )
  }
}