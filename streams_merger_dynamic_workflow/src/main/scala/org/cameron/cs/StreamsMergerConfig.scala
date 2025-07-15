package org.cameron.cs

import scopt.{OParser, OParserBuilder}

/**
 * Configuration parameters for StreamsMergerProcessor.
 *
 * @param execDate The execution date for the data processing. Typically used to identify the current batch of data.
 * @param prevExecDate The previous execution date. Used for identifying changes from the last run.
 * @param lowerBound The lower bound date for filtering data. Data older than this date will not be considered.
 * @param postsPath Path to the directory where post data is stored.
 * @param postsOffsetsPath Path to the directory where offsets for post data are stored.
 * @param metricsPath Path to the directory where metrics data is stored.
 * @param metricsOffsetsPath Path to the directory where offsets for metrics data are stored.
 * @param blogsPath Path to the directory where blog data is stored.
 * @param blogsOffsetsPath Path to the directory where offsets for blog data are stored.
 * @param mergedBlogsPath Path to the directory where merged blog data should be stored.
 * @param mergedPostsPath Path to the directory where merged post data should be stored.
 * @param skipTrash Indicates whether to skip the trash when deleting files. If "true", files will be deleted permanently.
 * @param batchSize The batch size for merging together posts with metrics
 */
case class StreamsMergerConfig(execDate: String           = "",
                               prevExecDate: String       = "",
                               lowerBound: String         = "",
                               postsPath: String          = "",
                               postsOffsetsPath: String   = "",
                               metricsPath: String        = "",
                               metricsOffsetsPath: String = "",
                               blogsPath: String          = "",
                               blogsOffsetsPath: String   = "",
                               mergedBlogsPath: String    = "",
                               mergedPostsPath: String    = "",
                               skipTrash: String          = "true",
                               batchSize: String          = "10")

/**
 * Configuration class for PostsStreamApp.
 *
 * Parses and holds configuration parameters provided via command line arguments.
 */
object StreamsMergerConfig {

  val builder: OParserBuilder[StreamsMergerConfig] = OParser.builder[StreamsMergerConfig]

  val parser: OParser[Unit, StreamsMergerConfig] = {

    import builder._

    OParser.sequence(
      programName("StreamsMergerApp"),
      head("StreamsMergerApp", "0.1"),

      opt[String]('d', "execDate")
        .action((x, c) => c.copy(execDate = x))
        .text("Execution date"),

      opt[String]("prevExecDate")
        .action((x, c) => c.copy(prevExecDate = x))
        .text("Previous execution date"),

      opt[String]("lowerBound")
        .action((x, c) => c.copy(lowerBound = x))
        .text("Lower bound date"),

      opt[String]('p', "postsPath")
        .action((x, c) => c.copy(postsPath = x))
        .text("Posts data path"),

      opt[String]("po")
        .action((x, c) => c.copy(postsOffsetsPath = x))
        .text("Posts offsets data path"),

      opt[String]('m', "metricsPath")
        .action((x, c) => c.copy(metricsPath = x))
        .text("Metrics data path"),

      opt[String]("mo")
        .action((x, c) => c.copy(postsOffsetsPath = x))
        .text("Metrics offsets data path"),

      opt[String]('b', "blogsPath")
        .action((x, c) => c.copy(blogsPath = x))
        .text("Blogs data path"),

      opt[String]("bo")
        .action((x, c) => c.copy(blogsOffsetsPath = x))
        .text("Blogs offsets data path"),

      opt[String]("mb")
        .action((x, c) => c.copy(mergedBlogsPath = x))
        .text("Merged blogs data path"),

      opt[String]("mp")
        .action((x, c) => c.copy(mergedPostsPath = x))
        .text("Merged posts data path"),

      opt[String] ("skipTrash")
        .action((x, c) => c.copy(skipTrash = x))
        .text("HDFS skipTrash param [removing permanently]"),

      opt[String]("batchSize")
        .action((x, c) => c.copy(batchSize = x))
        .text("Batch size for merging metrics with posts")
    )
  }
}