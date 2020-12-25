package cn.tongyongtao.Day10

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Test6 {
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
    //首先先不reducebykey,其实这种方法是多余的,只要初始的数据结构是(course,(name.number)就大可不必
    //触发一次行动算子算出科目去重
    val collect: Array[String] = initial.map(data => data._1._1).distinct.collect
    val partition = new MyPartition1(collect)
    val value: RDD[((String, String), Int)] = initial.reduceByKey(partition, _ + _)
    val res: RDD[((String, String), Int)] = value.mapPartitions(it => {
      //定义一个可排序的集合TreeSet
     // implicit val ord: Ordering[((String, String), Int)] = Ordering[Int].on[((String, String), Int)](t => t._2).reverse
      val sorter = new mutable.TreeSet[((String, String), Int)]()
      //变量迭代器
      it.foreach(t => {
        //将当前的这一条数据放入到treeSet中
        sorter.add(t)
        if (sorter.size > 3) {
          //移除最小的
          sorter -= sorter.last
        }
      })
      sorter.iterator
    })
    println(res.collect().toBuffer)
    sc.stop()
  }
}
class MyPartition1(val collect: Array[String]) extends Partitioner {
  private val rules = new mutable.HashMap[String, Int]()
  var index = 0
  //初始化一个分区的规则
  for (sb <- collect) {
    rules(sb) = index
    index += 1
  }

  override def numPartitions: Int = collect.size

  override def getPartition(key: Any): Int = {
    val value = key.asInstanceOf[(String, String)]._1
    rules(value)
  }
}
