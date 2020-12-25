package cn.tongyongtao.Day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val maprdd: RDD[(Int, Int)] = rdd.map(data => (data, 1))
    val result: RDD[(Int, Int)] = maprdd.partitionBy(new HashPartitioner(2))
        .partitionBy(new HashPartitioner(2))
    result.saveAsTextFile("src/aa")
    sc.stop()
  }
}
