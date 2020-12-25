package cn.tongyongtao.Day17

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd = sc.makeRDD(List("小明"))
    rdd.mapPartitions(data => {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      data.map(txt => {
        val date = format.parse(txt)
        (date.getTime,1)
      })
    })
  }
}
