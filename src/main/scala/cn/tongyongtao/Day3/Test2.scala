package cn.tongyongtao.Day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val test = new SparkConf().setMaster("local[*]").setAppName("test2")
    val sc = new SparkContext(test)
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6),2)
    //指定分区,">>>>" 打印了俩次,
    val mapp = rdd.mapPartitions(iter => {
      println(">>>>>>")
      iter.map(_ * 1)
    })
    mapp.collect().foreach(println)
    sc.stop()
  }
}
