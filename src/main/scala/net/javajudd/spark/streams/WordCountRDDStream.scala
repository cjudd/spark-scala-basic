package net.javajudd.spark.streams

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

// nc -lk 9999
object WordCountRDDStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WorkCountRDDStream")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
