package cn.tongyongtao.Day8

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
//partitionBy实现
object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    var rdd: RDD[(String, Int)] =sc.makeRDD(List(("A",1),("B",2),("D",9),("A",1)),2)
    val value: ShuffledRDD[String, Int, Int] = new ShuffledRDD[String, Int, Int](rdd, new HashPartitioner(rdd.partitions.length+1))
    println(value.partitions.length)
    sc.stop()
  }
}
