package net.javajudd.spark.rdd

import org.apache.spark.sql.SparkSession

import scala.util.Random

object RandomSort {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RandomSort")
      .getOrCreate()

    val data = Seq.fill(1000)(Random.nextInt)
    val rdd = spark.sparkContext.parallelize(data)

    println("Data:")
    rdd.foreach(println)

    val sortedRdd = rdd.sortBy(x => x)

    sortedRdd.collect().foreach(println)

    sortedRdd.saveAsTextFile("/tmp/count")
  }
}
