package cn.tongyongtao.Day10

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Test7 {
  def main(args: Array[String]): Unit = {
    //使用shuffleRDD
    //val result = countsAndSubjectTeacher.repartitionAndSortWithinPartitions(subjectPartitioner)
    //按照某种格式进行操作,然后重新采用另一种格式
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/topN.txt")
    val initial = rdd.map(data => {
      val result = data.split("/")
      val name = result(3)
      //注意是否包含首尾
      val course = result(2).substring(0, result(2).indexOf("."))
      ((course.toString, name.toString), 1)
    })

    //首先聚和
    val reducerdd = initial.reduceByKey(_ + _)
    //执行一次actor算子获取共有多少课程
    val collect: Array[String] = reducerdd.map(data => data._1._1).distinct.collect
    val ypartition = new MYpartition(collect)
    //使用元组的排序特性,重新格式化
    val value = reducerdd.map {
      case ((x, y), z) => ((z, (x, y)),null)
    }
    //按照第二元素重新分区,使其在同一个分区中
    implicit  val sort = Ordering[Int].on[((Int,(String,String)))](t => t._1).reverse
    //怎么定义shuffle的类型
    val value1 = new ShuffledRDD[(Int, (String, String)), Null, Null](value, ypartition)
    value1.setKeyOrdering(sort)
    value1.keys.collect.foreach(println)
  }
}

class MYpartition(val collect: Array[String]) extends Partitioner {
  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var label = 0
  for (th <- collect) {
    map(th) = label
    label += 1
  }

  override def numPartitions: Int = collect.size

  override def getPartition(key: Any): Int = {
    val value = key.asInstanceOf[(Int, (String, String))]._2._1
    map(value)
  }
}
