package cn.tongyongtao.Day7

import java.io

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test7 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("t").setMaster("local[*]"))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("A", 1), ("B", 8), ("C", 7), ("A", 7)))
    val rdd1 = sc.makeRDD(List(("A", "60"), ("A", "80"), ("C", "90"), ("F", "100")))
    val cagrdd: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd.cogroup(rdd1)
    //使用if来判断
    val value: RDD[(String, (Int, io.Serializable))] = cagrdd.flatMapValues(data => {

      if (data._1.isEmpty && data._2.nonEmpty) {
        data._2.map(data => (None, data))
      }
      if (data._1.nonEmpty && data._2.isEmpty) {
        data._1.map(data => (data, None))
      }
      else {
        for (vs <- data._1; ws <- data._2) yield (vs, ws)
      }
    })
    value.collect.foreach(println)
    sc.stop()
  }
}
