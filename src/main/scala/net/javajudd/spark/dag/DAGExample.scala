package net.javajudd.spark.dag

import org.apache.spark.sql.SparkSession

object DAGExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DAG Example")
      .getOrCreate()

    val ds1 = spark.range(1, 10000000)
    println(s"ds1 partition size: ${ds1.rdd.partitions.size}") // 16
    val ds2 = spark.range(1, 10000000, 2)
    println(s"ds2 partition size: ${ds2.rdd.partitions.size}") // 16
    val ds3 = ds1.repartition(7)
    println(s"ds3 partition size: ${ds3.rdd.partitions.size}") // 7
    val ds4 = ds2.repartition(9)
    println(s"ds4 partition size: ${ds4.rdd.partitions.size}") // 9
    val ds5 = ds3.selectExpr("id * 5 as id")
    println(s"ds5 partition size: ${ds5.rdd.partitions.size}") // 7
    val joined = ds5.join(ds4, "id")
    println(s"joined partition size: ${joined.rdd.partitions.size}") // 200
    val sum = joined.selectExpr("sum(id)")
    println(s"sum partition size: ${sum.rdd.partitions.size}") // 1
    sum.show()

    sum.explain()

    import org.apache.spark.sql.functions.spark_partition_id
    val joinedWithPartitionId = joined.withColumn("partition_id",spark_partition_id())
    joinedWithPartitionId.write.csv("/tmp/dag/joined")
  }
}
