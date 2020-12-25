package cn.tongyongtao.Day7

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("t").setMaster("local[*]"))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("A", 1), ("B", 8), ("C", 7), ("A", 7)))

    // def keys: RDD[K] = self.map(_._1)
    val keysrdd: RDD[String] = rdd.keys
    val valrdd: RDD[Int] = rdd.values
    val mvrdd: RDD[(String, Int)] = rdd.mapValues(data => data)
    keysrdd.collect.foreach(println)
    sc.stop()
  }
}
