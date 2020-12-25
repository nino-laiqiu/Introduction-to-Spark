package cn.tongyongtao.Day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
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
    //得到了每个分组的value值的总和
    val reducerdd = initial.reduceByKey(_ + _)
    //分治思想获取每个key._1的topN
    var list = List[String]("bigdata", "javaee")
    //分治排序方法一:sortby
    for (subject <- list) {
      /*      //过滤出每个科目的数据,排序获取take
            val filterrdd = reducerdd.filter(data => data._1._1 == subject)
            //按照value值来排序
            //sortby是如何排序的?默认是升序
            val tuples = filterrdd.sortBy(data => data._2, false).take(3).toList
            println(tuples)*/
      //分治排序方法二:方法二使用top取值
      //top的使用,是获取每个分区内的topN之后聚合取topN
      //首先过滤出每个分组的数据
      val filterrdd: RDD[((String, String), Int)] = reducerdd.filter(data => data._1._1 == subject)
      implicit val sort = Ordering[Int].on[((String, String), Int)](t => t._2)
      //top是使原本降序的规则升序
      val result = filterrdd.top(3).toList
      println(result)
    }
    sc.stop()
  }
}
