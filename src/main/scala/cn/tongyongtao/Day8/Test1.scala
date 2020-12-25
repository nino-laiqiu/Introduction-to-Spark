package cn.tongyongtao.Day8

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Wordcount11种实现方式
object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd: RDD[String] = sc.makeRDD(List("hello scala","hello spark"))
    wordcount1(rdd)
    sc.stop()
  }
  //groupbykey
  def wordcount1(rdd :RDD[String])={
    val value: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(data => (data, 1))
    val value1: RDD[(String, Int)] = value.groupByKey().map(data => (data._1, data._2.size))
    value1.collect.foreach(println)
  }
  //groupby
  def wordcount2(rdd:RDD[String])={
    val value: RDD[(String, Iterable[String])] = rdd.flatMap(_.split(" ")).groupBy(data => data)
    val value1: RDD[(String, Int)] = value.mapValues(data => data.size)
    value1.collect.foreach(println)
  }
  //reducebukey
  def wordcount3(rdd :RDD[String])={

  }
  //aggregatbykey
  //foldbykey
  //conbinerbykey
  //countbykey
  //countbuvalue
  //reduce
  //aggreate
  //fold
}
