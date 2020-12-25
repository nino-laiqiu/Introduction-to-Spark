/*package cn.tongyongtao.Day4

import java.text.SimpleDateFormat
import java.util.{Date, SimpleTimeZone}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    //求取分区的最大值的和
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    //读取文本获取,时间内的点击量
    val rdd = sc.textFile("src/main/resources/aa.txt")
    val value: RDD[(String, Int)] = rdd.map(data => {
      val str: Array[String] = data.split(" ")
      //获取时间
      val str1 = str(3)
      val format = new SimpleDateFormat("dd/mm/yyyy:HH:MM:SS")
      val str2: Date = format.parse(str1)
      val format1 = new SimpleDateFormat("HH")
      val str3: String = format1.format(str2)
      (str3, 1)
    }).groupBy(_._1).map(data => (data._1, data._2.size))
    value.collect.foreach(println)
    sc.stop()
  }
}*/
