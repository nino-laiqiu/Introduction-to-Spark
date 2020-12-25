package cn.tongyongtao.Day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("...").setMaster("local[*]"))
    //交集
    val S1 = sc.makeRDD(List(1, 2, 3, 4))
    val S2 = sc.makeRDD(List(3, 4, 5, 6))
    val result = S1.zip(S2)
    result.collect.foreach(println)
    sc.stop()


  }
}
