package cn.tongyongtao.Day7

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(("小明", "1,2,3"), ("小明", "4,5,6"), ("小莫", "2,3,4")),2)
    val result: RDD[Array[(String, String)]] = rdd.map(data => {
      data match {
        case (x, y) => {
          //返回的是数组
          val tuples: Array[(String, String)] = y.split(",").map(data => (x, data))
          tuples
        }
      }
    })
    val value: RDD[(String, String)] = result.flatMap(data => data)
      value.collect.foreach(println)
     //result.collect.foreach(println)
    sc.stop()
  }
}
