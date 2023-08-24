package net.javajudd.spark.rdd

import org.apache.spark.sql.SparkSession

object SimpleRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleRDD")
      .master("local")
      .getOrCreate()

    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = spark.sparkContext.parallelize(data)

    println("Data:")
    rdd.foreach(println)

    println(s"count: ${rdd.count()}")

    rdd.saveAsTextFile("/tmp/languages")
  }
}
