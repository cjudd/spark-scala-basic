package net.javajudd.spark.rdd

import org.apache.spark.sql.SparkSession

object SimpleRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleRDD")
      .getOrCreate()

    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = spark.sparkContext.parallelize(data)

    println("Data:")
    rdd.foreach(println)

    println(s"count: ${rdd.count()}")

    val sortedRdd = rdd.sortByKey(numPartitions = 1)
    //val sortedRdd = rdd.sortByKey().collect() // should not be used with large result sets
    println("Sorted:")
    sortedRdd.foreach(println)

    val groupByRdd = rdd.groupBy(_._2)
    groupByRdd.foreach(println)

    rdd.saveAsTextFile("/tmp/languages")
  }
}
