package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import MetricsStreamConfig._
import scopt.OParser

object MetricsStreamApp extends App {

  val conf: MetricsStreamConfig =
    OParser.parse(parser, args, MetricsStreamConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  val processor = new MetricsStreamProcessor(spark, conf)

  processor.process()
}
