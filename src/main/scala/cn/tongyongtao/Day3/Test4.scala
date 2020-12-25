package cn.tongyongtao.Day3

import org.apache.spark.{SparkConf, SparkContext}
//
object Test4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test4").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD = sc.makeRDD(List[Int](1, 2, 3, 4),2)
    val map = RDD.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      }
      else {
        Nil.iterator
      }
    })
    map.collect.foreach(println)
    sc.stop()
  }
}
