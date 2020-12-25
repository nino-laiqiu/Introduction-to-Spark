package cn.tongyongtao.Day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/topN.txt")
    val initial = rdd.map(data => {
      val result = data.split("/")
      val name = result(3)
      val course = result(2).substring(0, result(2).indexOf("."))
      ((course.toString, name.toString), 1)
    })
    val reducerdd = initial.reduceByKey(_ + _)
    //分组相同的key必然落到同一个分区!!!
    //使用set的自动排序
    val grouprdd: RDD[(String, Iterable[(String, Int)])] = reducerdd.map(data => (data._1._1, (data._1._2, data._2))).groupByKey()
    //每个组一个set
    val value: RDD[(String, List[(String, Int)])] = grouprdd.mapValues(data => {
      //隐式转换,降序
      implicit val sortedSet = Ordering[Int].on[(String, Int)](t => t._2).reverse
      val set = mutable.Set[(String, Int)]()
      data.foreach(tr => {
        set += tr
        if (set.size > 3) {
          set -= set.last
        }
      })
      set.toList.sortBy(x => -x._2)
    })
    value.collect.foreach(println)
    sc.stop()
  }
}
