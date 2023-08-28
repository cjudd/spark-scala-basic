package net.javajudd.spark.ds

import org.apache.spark.sql.SparkSession

case class Book(id: Int, author: String, title: String, releaseDate: String, link: String)

object BookCSVAuthorsDS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Book CSV Count Authors Dataset")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val input = args(0)
    val output = args(1)

    import spark.implicits._
    val booksDS = spark.read
      .options(Map(
        "header"->"true",
        "inferSchema"->"true",
        "delimiter"->";",
        "quote"->"*"))
      .csv(input)
      .as[Book]

    //val books: Array[Book] = booksDS.collect()

    booksDS.printSchema()
    booksDS.show()

    booksDS
      .select("author", "title")
      .orderBy("author")
      .show(10)

    import spark.implicits._
    booksDS
      .groupBy("author")
      .count()
      .orderBy("author")
      .filter($"count" > 2)
      .show(10)

    booksDS.createOrReplaceTempView("books")
    spark
      .sql("SELECT author, count(id) FROM books group by author order by author")
      .show(10)

    booksDS.createOrReplaceTempView("books")
    spark
      .sql("SELECT author, count(id) FROM books group by author order by author")
      .repartition(1)
      .write
        .option("header",true)
        .csv(output)
  }
}
