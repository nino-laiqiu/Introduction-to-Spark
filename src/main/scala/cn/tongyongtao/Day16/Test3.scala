package cn.tongyongtao.Day16

import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val value = sc.makeRDD(List((1, 2), (1, 3), (1, 4)),1)
    val value1 = sc.makeRDD(List((1, 2), (1, 3), (1, 4)),1)
    val value2 = value.join(value1, 1)
    value2.collect().foreach(println)
    Thread.sleep(Int.MaxValue)
    sc.stop()
  }
}
