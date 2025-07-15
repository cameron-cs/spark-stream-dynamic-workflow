package org.cameron.cs.metrics

import org.apache.spark.sql.types._

object MetricsSchema {

  val schema = new StructType()
    .add("blogtype", LongType, nullable = true)
    .add("blogurl", StringType, nullable = true)
    .add("partdate", StringType, nullable = true)
    .add("comments", LongType, nullable = true)
    .add("likes", LongType, nullable = true)
    .add("reposts", LongType, nullable = true)
    .add("views", LongType, nullable = true)
    .add("commentnegativecount", LongType, nullable = true)
    .add("commentpositivecount", LongType, nullable = true)
    .add("metricstimestamp", StringType, nullable = true)
    .add("metricstype", LongType, nullable = true)
    .add("metricsurl", StringType, nullable = true)
    .add("currenttimestamp", TimestampType, nullable = true)
    .add("kafkapartition", IntegerType, nullable = true)
    .add("kafkaoffset", LongType, nullable = true)
    .add("id", StringType, nullable = true)
    .add("execdate", StringType, nullable = true)

}
