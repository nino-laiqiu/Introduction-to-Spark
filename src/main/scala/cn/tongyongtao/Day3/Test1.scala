package cn.tongyongtao.Day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val v: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),2)
    val v1 = v.map(data => {
      println(">>>>>>>" + data)
      data
    })
    val v2 = v1.map(data => {
      println("!!!!" + data)
      data
    })
    v2.collect()
    sc.stop()
  }
}
