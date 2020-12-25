package cn.tongyongtao.Day6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//21. 13班数学最高成绩是多少？

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/sanguo.txt")
    val strings: Int = rdd.map(_.split(" ")).filter(_ (0) == "13").filter(_ (4) == "math").map(_ (5).toInt).max()
      //.sortBy(_.toInt,false).take(1)
    println(strings)
    sc.stop()
  }
}
