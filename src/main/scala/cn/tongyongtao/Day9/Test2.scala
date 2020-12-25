package cn.tongyongtao.Day9

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("jdany").setMaster("local[*]"))
    val rdd: RDD[String] = sc.textFile("src/main/resources/user_visit_action.txt")
    val result: RDD[(String, (Int, Int, Int))] = rdd.flatMap(data => {
      val strip = data.split("_")
      if (strip(6) != "-1") {
        List((strip(6), (1, 0, 0)))
      }
      else if (strip(8) != "null") {
        strip(8).split(",").map(data => (data, (0, 1, 0)))
      }
      else if (strip(10) != "null") {
        strip(10).split(",").map(data => (data, (0, 0, 1)))
      }
      else {
        Nil
      }
    })
    val value = result.reduceByKey((x, x1) => {
      (x._1 + x1._1, x._2 + x1._2, x._3 + x1._3)
    })
    val tuples = value.sortBy(_._2, false).take(10)
    println(tuples.mkString(","))
    sc.stop()
  }
}
