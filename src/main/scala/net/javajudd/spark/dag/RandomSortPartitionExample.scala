package net.javajudd.spark.dag

import org.apache.spark.sql.SparkSession
import scala.util.Random

object RandomSortPartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RandomSortPartitionExample")
      .getOrCreate()

    val data = Seq.fill(1000)(Random.nextInt)
    val rdd = spark.sparkContext.parallelize(data)

    println(s"rdd partition size: ${rdd.partitions.size}") // 16

    import spark.implicits._
    import org.apache.spark.sql.functions.spark_partition_id
    val dfWithPartitionId = rdd.toDF.withColumn("partition_id", spark_partition_id)
    dfWithPartitionId.show()

    val coalescedRdd = rdd.coalesce(2)
    println(s"coalescedRdd partition size: ${coalescedRdd.partitions.size}") // 2

    println("Data:")
    rdd.foreach(println)

    val sortedRdd = rdd.sortBy(x => x)

    sortedRdd.collect().foreach(println)

    //sortedRdd.saveAsTextFile("/tmp/count")
  }
}
