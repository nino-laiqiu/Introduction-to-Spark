package cn.tongyongtao.Day7

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(("小明", "1,2,3"), ("小明", "4,5,6"), ("小莫", "2,3,4")))
    val mvrdd: RDD[(String, Array[String])] = rdd.mapValues(data => data.split(","))
    val maprdd: RDD[(String, Int)] = mvrdd.map(data => (data._1, data._2.map(data => data.toInt).sum))
    val result: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
     result.collect.foreach(println)
    sc.stop()
  }
}
