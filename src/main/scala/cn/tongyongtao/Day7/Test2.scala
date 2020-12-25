package cn.tongyongtao.Day7

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(("小明", "1,2,3"), ("小明", "4,5,6"), ("小莫", "2,3,4")))
    //分组求总和
    val value: RDD[(String, Iterable[String])] = rdd.groupByKey()
    val value1: RDD[(String, Iterable[Array[Int]])] = value.mapValues(data => {
      data.map(data => {
        val strings: Array[String] = data.split(",")
        val ints: Array[Int] = strings.map(data => data.toInt)
        ints
      })
    })
    val result = value1.map(data => (data._1, data._2.map(_.sum).sum))
    result.collect.foreach(println)
    sc.stop()
  }
}
