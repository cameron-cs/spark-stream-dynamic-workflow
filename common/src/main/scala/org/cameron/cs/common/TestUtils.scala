package org.cameron.cs.common

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.nio.file.Paths
import java.security.MessageDigest
import java.util.Properties
import scala.io.Source
import scala.sys.process.Process

object TestUtils extends Logging {

  private def runDockerComposeCommand(command: Seq[String],
                                      composeDirPath: String = ""): Unit = {
    val composeDir =
      if (composeDirPath.isEmpty) Paths.get("").toAbsolutePath.toString
      else Paths.get(composeDirPath).toAbsolutePath.toString

    logInfo(s"Running '${command.mkString(" ")}' in $composeDir")
    val processBuilder = Process(command, new java.io.File(composeDir))
    val exitCode = processBuilder.!

    val cmd = command.mkString(" ")
    if (exitCode == 0)
      logInfo(s"'$cmd' command succeeded")
    else
      logError(s"'$cmd' command failed with exit code $exitCode")
  }

  def readResourceFileAsString(path: String): String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(path))
      .getOrElse(sys.error(s"Resource not found: $path"))

    val source = Source.fromInputStream(stream)
    try source.getLines().mkString("\n") finally source.close()
  }

  def readResourceTextFile(path: String): String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(path))
      .getOrElse(sys.error(s"Resource not found: $path"))

    val source = Source.fromInputStream(stream)
    try source.getLines().mkString("\n") finally source.close()
  }

  def dockerComposeUp(composeDirPath: String = ""): Unit =
    runDockerComposeCommand(Seq("docker-compose", "up", "-d"), composeDirPath)

  def dockerComposeDown(composeDirPath: String = ""): Unit =
    runDockerComposeCommand(Seq("docker-compose", "down"), composeDirPath)

  def dummyHashString(input: String, algorithm: String = "MD5"): String = {
    val digest = MessageDigest.getInstance(algorithm)
    val hashBytes = digest.digest(input.getBytes("UTF-8"))
    // convert bytes to hex string
    hashBytes.map("%02x".format(_)).mkString
  }

  def defaultValue(dataType: DataType): Any = dataType match {
    case StringType => ""
    case IntegerType => 0
    case LongType => 0L
    case DoubleType => 0.0
    case FloatType => 0.0f
    case BooleanType => false
    case TimestampType => new java.sql.Timestamp(0L)
    case DateType => new java.sql.Date(0L)
    case ArrayType(elementType, _) => Seq(defaultValue(elementType))
    case StructType(fields) => Row.fromSeq(fields.toSeq.map(f => defaultValue(f.dataType)))
    case _ => null // fallback for unsupported types
  }

  def generateRowsWithDefaults(spark: SparkSession, schema: StructType, count: Int): DataFrame = {
    val rows = spark.sparkContext.parallelize(1 to count).map { _ =>
      val values = schema.fields.map(f => defaultValue(f.dataType))
      Row.fromSeq(values.toSeq)
    }
    spark.createDataFrame(rows, schema)
  }

  def kafkaProps(kafkaHost: String = "localhost:9092"): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", kafkaHost))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all") // wait for all replicas to ack
    props.put("retries", "3") // retry up to 3 times
    props.put("linger.ms", "5") // small batching delay
    props
  }

  def sendToKafkaRecords(jsonStrings: Seq[String], blogsTopicName: String): Unit = {
    val props = kafkaProps()
    val producer = new KafkaProducer[String, String](props)

    // send each JSON row as a Kafka message
    jsonStrings.foreach { json =>
      val record = new ProducerRecord[String, String](blogsTopicName, null, json)
      producer.send(record)
    }

    producer.flush()
    producer.close()
    logInfo("The synthetic test data sent to Kafka successfully!")
  }

  def sharedSparkSession(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
}