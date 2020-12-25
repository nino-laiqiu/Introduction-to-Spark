package cn.tongyongtao.Day2

import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("src/main/resources/aa.txt",2)
      .saveAsTextFile("src/main/aa")
       sc.stop()

  }
}
