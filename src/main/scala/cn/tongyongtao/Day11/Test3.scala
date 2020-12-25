package cn.tongyongtao.Day11

import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/top.txt",3)
    val value = rdd.map(data => {
      val strings = data.split(" ")
      (strings(1), 1)
    })

    val value1 = value.reduceByKey((x, x1) => {
      println("+++")
      x + x1
    })
    value1.collect.foreach(println)
    sc.stop()

  }
}
