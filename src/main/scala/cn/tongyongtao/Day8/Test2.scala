package cn.tongyongtao.Day8

import org.apache.spark.{SparkConf, SparkContext}

import scala.runtime.Nothing$

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("test5").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5))
    val rdd2 = sc.makeRDD(List(4, 5, 6, 7, 8))
    val rdd = rdd2.map(data => (data, null))
    val result = rdd1.map(data => (data, null)).cogroup(rdd).filter {
      case (_, (ws, vs)) => ws.nonEmpty && vs.nonEmpty
    }.keys
    result.collect.foreach(println)
    sc.stop()
  }
}
