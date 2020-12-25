package cn.tongyongtao.Day3

import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6),2)
    val mapprdd = RDD.mapPartitions(iter => {
      println(">>>>>")
      List[Int](iter.max).iterator
    })
    mapprdd.collect.foreach(println)
    sc.stop()
  }
}
