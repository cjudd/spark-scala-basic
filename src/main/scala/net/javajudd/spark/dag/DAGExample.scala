package net.javajudd.spark.dag

import org.apache.spark.sql.SparkSession

object DAGExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DAG Example")
      .getOrCreate()

    val ds1 = spark.range(1, 10000000)
    val ds2 = spark.range(1, 10000000, 2)
    val ds3 = ds1.repartition(7)
    val ds4 = ds2.repartition(9)
    val ds5 = ds3.selectExpr("id * 5 as id")
    val joined = ds5.join(ds4, "id")
    val sum = joined.selectExpr("sum(id)")
    sum.show()
  }
}
