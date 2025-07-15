package org.cameron.cs

import scopt.{OParser, OParserBuilder}

/**
 * Configuration class for the PostsStreamProcessor.
 * This class holds all necessary configurations needed by the PostsStreamProcessor to read from Kafka and write to HDFS.
 *
 * @param execDate The execution date for the current processing job.
 * @param kafkaHost Kafka host information.
 * @param kafkaConsumerGroup Kafka consumer group ID.
 * @param postsTopicName Name of the Kafka topic to read posts from.
 * @param hdfsPath Path in HDFS where the processed data should be written.
 * @param hdfsOffsetsPath Path in HDFS to store and read offsets.
 */
case class PostsStreamConfig(execDate: String           = "",
                             kafkaHost: String          = "",
                             kafkaConsumerGroup: String = "",
                             postsTopicName: String     = "",
                             hdfsPath: String           = "",
                             hdfsOffsetsPath: String    = "",
                             excludes: String           = "",
                             primary: String           = "")

/**
 * Companion object for the PostsStreamConfig case class.
 * This object contains the command-line argument parser that fills in the PostsStreamConfig.
 * It uses the scopt library to define and parse command-line options, providing a user-friendly way to configure the PostsStreamProcessor.
 */
object PostsStreamConfig {

  val builder: OParserBuilder[PostsStreamConfig] = OParser.builder[PostsStreamConfig]

  val parser: OParser[Unit, PostsStreamConfig] = {

    import builder._

    OParser.sequence(
      programName("PostsStreamApp"),
      head("PostsStreamApp", "0.1"),

      opt[String]('d', "execDate")
        .action((x, c) => c.copy(execDate = x))
        .text("Execution date"),

      opt[String]('h', "kafkaHost")
        .action((x, c) => c.copy(kafkaHost = x))
        .text("Kafka hosts"),

      opt[String]('g', "kafkaConsumerGroup")
        .action((x, c) => c.copy(kafkaConsumerGroup = x))
        .text("Kafka Consumer Group"),

      opt[String]('t', "kafkaPostsTopicName")
        .action((x, c) => c.copy(postsTopicName = x))
        .text("Kafka Posts topic name"),

      opt[String]('p', "hdfsPath")
        .action((x, c) => c.copy(hdfsPath = x))
        .text("Path to HDFS"),

      opt[String]('o', "hdfsOffsetsPath")
        .action((x, c) => c.copy(hdfsOffsetsPath = x))
        .text("Offsets path to HDFS"),

      opt[String]('e', "excludes")
        .action((x, c) => c.copy(excludes = x))
        .text("Excludes for posts"),

      opt[String]("primary")
        .action((x, c) => c.copy(primary = x))
        .text("Primary columns for posts")
    )
  }
}