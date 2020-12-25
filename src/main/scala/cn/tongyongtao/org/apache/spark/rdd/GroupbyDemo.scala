package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object GroupbyDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(List(("spark", 1), ("scala", 2), ("kafka", 4), ("flink", 9), ("spark", 1)))

  }
}
