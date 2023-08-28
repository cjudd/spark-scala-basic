package net.javajudd.spark.df

import org.apache.spark.sql.SparkSession

object BookCSVAuthorsDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Book CSV Count Authors DataFrames")
      .getOrCreate()

    val input = args(0)

    val books = spark.read
      //.format("csv")
      .options(Map(
        "header"->"true",
        "inferSchema"->"true",
        "delimiter" -> ";"))
      .csv(input)

    books.printSchema()
    books.show()
  }
}
