package net.javajudd.spark.streams

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// nc -lk 9999
object WordCountDFStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WordCountDFStream")
      .getOrCreate()

    val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    df.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("csv")
      .option("path", "/tmp/logstream")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start().awaitTermination()
  }
}
