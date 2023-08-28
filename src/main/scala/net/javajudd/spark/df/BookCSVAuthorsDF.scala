package net.javajudd.spark.df

import org.apache.spark.sql.SparkSession

object BookCSVAuthorsDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Book CSV Count Authors DataFrames")
      .getOrCreate()

    val input = args(0)
    val output = args(1)

    val booksDF = spark.read
      //.format("csv").load(input)
      .options(Map(
        "header"->"true",
        "inferSchema"->"true",
        "delimiter"->";",
        "quote"->"*"))
      .csv(input)

    booksDF.printSchema()
    booksDF.show()

    booksDF
      .select("author", "title")
      .orderBy("author")
      .show(10)

    import spark.implicits._
    booksDF
      .groupBy("author")
      .count()
      .orderBy("author")
      .filter($"count" > 2)
      .show(10)

    booksDF.createOrReplaceTempView("books")
    spark
      .sql("SELECT author, count(id) FROM books group by author order by author")
      .show(10)

    booksDF.createOrReplaceTempView("books")
    spark
      .sql("SELECT author, count(id) FROM books group by author order by author")
      .repartition(1)
      .write
        .option("header",true)
        .csv(output)
  }
}
