package cn.tongyongtao.Day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable

object Test5 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    //分组topN
    val rdd = sc.textFile("src/main/resources/topN.txt")
    //格式化数据
    val initial = rdd.map(data => {
      val result = data.split("/")
      val name = result(3)
      //注意是否包含首尾
      val course = result(2).substring(0, result(2).indexOf("."))
      ((course.toString, name.toString), 1)
    })
    //首先聚合
    val aggrdd: RDD[((String, String), Int)] = initial.aggregateByKey(0)(_ + _, _ + _)
    //触发一次actor获取科目的总数
    val collect: Array[String] = aggrdd.map(data => data._1._1).distinct.collect
    //方法五基于方法四要groupbykey会产生shuffle的优化,使用自定义分区器,有点避免了shuffle但是增加了task
    val mypartitioner = new Mypartitioner(collect)
    val pardd: RDD[((String, String), Int)] = aggrdd.partitionBy(mypartitioner)
    //已经重新分区了,每个分区只有特定的key值
    //一批一批传输
    val value: RDD[((String, String), Int)] = pardd.mapPartitions(data => {
      implicit val sout = Ordering[Int].on[((String, String), Int)](t => t._2)
      val set = mutable.TreeSet[((String, String), Int)]()
      data.foreach(it => {
        set += it
        if (set.size > 3) {
          set.dropRight(1)
        }
      })
      set.toList.iterator
    })
    val value1= value.map(data => {
      println(data)
      data
    })
     value1.collect.foreach(println)
    //value.foreach(println)
  }
}

class Mypartitioner(val collect: Array[String]) extends Partitioner {
  private val stringToInt = new mutable.HashMap[String, Int]()
  var label = 0
  for (key <- collect) {
    stringToInt(key) = label
    label += 1
  }

  override def numPartitions: Int = collect.length

  override def getPartition(key: Any): Int = {
    //获取科目
    val i = key.asInstanceOf[(String, String)]._1
    stringToInt(i)
  }
}
