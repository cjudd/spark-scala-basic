package net.javajudd.spark.rddtodf

import org.apache.spark.sql.SparkSession

object SimpleRDDToDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleClient")
      .getOrCreate()

    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = spark.sparkContext.parallelize(data)

    val df = spark.createDataFrame(rdd)

    df.printSchema()
    df.show()

    rdd.saveAsTextFile("/tmp/language")
  }
}
