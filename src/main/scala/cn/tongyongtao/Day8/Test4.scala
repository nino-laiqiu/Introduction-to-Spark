package cn.tongyongtao.Day8

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.{immutable, mutable}

object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(List(1, 2,2,3, 3, 4, 5))
    val rdd2 = sc.makeRDD(List(4, 5, 5, 6, 7))
    val map1rdd1: RDD[(Int, Iterable[Null])] = rdd1.map(data => (data, null)).groupByKey()
    val map1rdd2: RDD[(Int, Iterable[Null])] = rdd2.map(data => (data, null)).groupByKey()
    var hash = mutable.HashMap[Int, Iterable[Null]]()
    map1rdd1.collect.foreach(data => hash += data)
    map1rdd2.collect.foreach(data => hash.remove(data._1) )
    println(hash)
    val list: immutable.Seq[(Int, Null)] = hash.toList.flatMap(data => {
      data._2.map(data1 => (data._1, data1))
    })
    list.map(data => data._1).foreach(println)
    sc.stop()
  }
}
