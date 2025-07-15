package org.cameron.cs.common.kafka

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * Class responsible for writing Kafka offsets to HDFS.
 *
 * @param spark The SparkSession instance.
 */
class SparkKafkaOffsetsWriter(spark: SparkSession) extends Logging {

  import spark.implicits._

  /**
   * Saves the Kafka offsets DataFrame to HDFS with execution date information.
   *
   * @param rawDataFrame    The DataFrame containing Kafka offsets.
   * @param isFirstRun      Indicates whether it's the first run of the job.
   * @param topicName       The name of the Kafka topic.
   * @param consumerGroup   The consumer group associated with the Kafka offsets.
   * @param execDate        The execution date.
   * @param execTime        The execution time.
   * @param hdfsOffsetsPath The HDFS path where the offsets will be saved.
   *
   * A transformation applied to a DataFrame representing Kafka offsets.
   * It aggregates the maximum Kafka offset for each Kafka partition and adds additional
   * metadata columns such as topic, consumergroup, execdate, and exectime.
   *
   * - kafkapartition  The Kafka partition of the the Kafka topic
   * - latestoffset    The latest offset of the Kafka topic
   * - topic           The name of the Kafka topic associated with the offsets.
   * - consumergroup   The consumer group associated with the Kafka offsets.
   * - execdate        The execution date metadata value to be added.
   * - exectime        The execution time metadata value to be added.
   */
  def saveOffsetsWithExecDate(rawDataFrame: DataFrame)
                             (isFirstRun: Boolean,
                              topicName: String,
                              consumerGroup: String,
                              execDate: String,
                              execTime: String,
                              hdfsOffsetsPath: String,
                              groupByColumn: String = "kafkapartition",
                              aggColumn: String = "kafkaoffset"): Unit = {
    val offsetsDF =
      rawDataFrame.groupBy(col(groupByColumn))
        .agg(functions.max(col(aggColumn)) as "latestoffset")
        .withColumn("topic", lit(topicName))
        .withColumn("consumergroup", lit(consumerGroup))
        .withColumn("execdate", lit(execDate))
        .withColumn("exectime", lit(execTime))

    if (isFirstRun)
      offsetsDF
        .write
        .mode("overwrite")
        .parquet(hdfsOffsetsPath)
    else
      offsetsDF
        .write
        .mode("append")
        .format("parquet")
        .save(hdfsOffsetsPath)
  }
}
