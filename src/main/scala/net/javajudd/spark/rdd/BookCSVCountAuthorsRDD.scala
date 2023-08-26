package net.javajudd.spark.rdd

import org.apache.spark.sql.SparkSession

object BookCSVCountAuthorsRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Book CSV Count Authors")
      .getOrCreate()

    val csvRdd = spark.sparkContext.textFile(args(0))
    val authorGroupRdd = csvRdd
      .map(row => {
        val fields = row.split(";").map(_.trim)
        (fields(1), 1)
      })
      .filter(_._1 != "author")
      .reduceByKey((x, y) => x + y)
      .sortByKey()

    authorGroupRdd.foreach(println)
    authorGroupRdd.saveAsTextFile(args(1))
  }
}
