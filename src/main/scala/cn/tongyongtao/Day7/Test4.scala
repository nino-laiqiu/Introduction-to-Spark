package cn.tongyongtao.Day7

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(("小明", "1,2,3"), ("小明", "4,5,6"), ("小莫", "2,3,4")),5)
    val fmvrdd: RDD[(String, Int)] = rdd.flatMapValues(data => data.split(",").map(_.toInt))
    val value: RDD[(String, Int)] = fmvrdd.reduceByKey(_ + _)
    value.collect.foreach(println)
    sc.stop()
  }
}
