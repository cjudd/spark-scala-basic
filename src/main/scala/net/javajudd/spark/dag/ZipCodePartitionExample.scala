package net.javajudd.spark.dag

import org.apache.spark.sql.SparkSession

import scala.util.Random

object ZipCodePartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ZipCodePartitionExample")
      .getOrCreate()

    val randomData = Seq.fill(1000)(Random.nextInt)
    val randomRdd = spark.sparkContext.parallelize(randomData)
    println(s"Default number of partitions: ${randomRdd.getNumPartitions}") // 16

    val zipsDF = spark.read
      .options(Map(
        "header" -> "true",
        "inferSchema" -> "true"))
      .csv("data/uszips.csv")

    println(s"zip number of partitions: ${zipsDF.rdd.getNumPartitions}") // 2

    zipsDF.write
      .option("header", true)
      .partitionBy("state_id")
      .mode("overwrite")
      .csv("/tmp/zipcodes-states")
  }
}
