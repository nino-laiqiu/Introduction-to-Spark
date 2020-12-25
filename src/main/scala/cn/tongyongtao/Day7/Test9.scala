package cn.tongyongtao.Day7

import org.apache.spark.{SparkConf, SparkContext}

object Test9 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test10").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(4, 5, 6, 1, 2, 3))
    //reduce聚合
    println(rdd.reduce(_ + _))
    //collect 收集返回数组类型
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))
    //count求总的次数
    println(rdd.count())
    println(rdd.sum())
    //获取第一个
    println(rdd.first())
    //获取前topN
    val ints1: Array[Int] = rdd.take(2)
    println(ints1.mkString(","))
    //获取排序过的topN
    rdd.takeOrdered(3).foreach(println)
    //排序,指定排序规则
    rdd.takeOrdered(3)(Ordering.Int.reverse).foreach(println)
    //
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)//Map(5 -> 1, 1 -> 1, 6 -> 1, 2 -> 1, 3 -> 1, 4 -> 1)
  }
}
