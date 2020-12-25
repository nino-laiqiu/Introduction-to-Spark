package cn.tongyongtao.Day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test4").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(("A", 1), ("A", 2), ("A", 3), ("A", 4), ("B", 1)))
    val result1: RDD[(String, Int)] = rdd.reduceByKey((s1, s2) => s1 + s2)
    val result2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val result3: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

  }
}
