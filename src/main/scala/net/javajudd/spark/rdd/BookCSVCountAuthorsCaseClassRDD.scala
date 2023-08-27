package net.javajudd.spark.rdd

import org.apache.spark.sql.SparkSession

case class Book(id: Int, author: String, title: String, releaseDate: String, link: String)

object BookCSVCountAuthorsCaseClassRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Book CSV Count Authors")
      .getOrCreate()

    val csvRdd = spark.sparkContext.textFile(args(0))
    val authorGroupRdd = csvRdd
      .filter(!_.contains("id;author;title;releaseDate;link"))
      .map(row => {
        val fields = row.split(";").map(_.trim)
        Book(fields(0).toInt, fields(1), fields(2), fields(3), fields(4))
      })
      .map(book => {
        (book.author, 1)
      })
      .reduceByKey((x, y) => x + y)
      .sortByKey()

    authorGroupRdd.foreach(println)
    authorGroupRdd.saveAsTextFile(args(1))
  }
}
