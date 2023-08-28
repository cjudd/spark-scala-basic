package net.javajudd.spark.dftordd

import org.apache.spark.sql.SparkSession

object SimpleDFToRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleClient")
      .getOrCreate()

    import spark.implicits._
    val dataDF = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000)).toDF()

    val dataRDD = dataDF.rdd
    dataRDD.saveAsTextFile("/tmp/languages")
  }
}
