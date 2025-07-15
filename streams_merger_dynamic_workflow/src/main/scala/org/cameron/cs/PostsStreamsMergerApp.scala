package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import StreamsMergerConfig.parser
import org.cameron.cs.common.KeyGenerator
import scopt.OParser

object PostsStreamsMergerApp extends App {

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
  val processor = new StreamsMergerProcessor(spark, conf, mergerUtils)

  val urlHash = udf { url: String => KeyGenerator.getKey(url) }
  processor.processMergePosts(urlHash)
}
