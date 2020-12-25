package cn.tongyongtao.Day7

import java.io

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test6 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("t").setMaster("local[*]"))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("A", 1), ("B", 8), ("C", 7), ("A", 7)))
    val rdd1 = sc.makeRDD(List(("A", "60"), ("A", "80"), ("C", "90"), ("F", "100")))
    //实现leftoutjoin
    val cagrdd: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd.cogroup(rdd1)
    val value = cagrdd.flatMapValues(data => {
      data match {
        case (vs, Seq()) => vs.iterator.map(data => (data, None))
        case (Seq(),ws) => ws.iterator.map(data => (None,data))
        case (vs, ws) => for (x <- vs.iterator; y <- ws.iterator) yield (x, y)
      }
    })
    value.collect.foreach(println)
    sc.stop()
  }
}
