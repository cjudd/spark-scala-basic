package net.javajudd.spark.rddtodf

import org.apache.spark.sql.SparkSession

object SimpleRDDToDFSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleClient")
      .master("local")
      .getOrCreate()

    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = spark.sparkContext.parallelize(data)

    import spark.implicits._
    val df = rdd.toDF("language", "users_count")

    df.printSchema()
    df.show()

    rdd.saveAsTextFile("/tmp/language")
  }
}
