package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import StreamsMergerConfig.parser
import scopt.OParser

object PostsAttachmentsMergerApp extends App {

  val conf: StreamsMergerConfig =
    OParser.parse(parser, args, StreamsMergerConfig()) match {
      case Some(config) => config
      case _ => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val mergerUtils = new StreamMergerUtils(spark)
  val processor = new PostsStructureProcessor(spark, conf, mergerUtils, "/data")
  processor.processMergePostsWithAttachments()
}
