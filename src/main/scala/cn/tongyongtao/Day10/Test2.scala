package cn.tongyongtao.Day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//topN案例,形如如下数据
//http://javaee.51doit.cn/xiaozhang
//http://javaee.51doit.cn/laowang
//http://javaee.51doit.cn/laowang
//http://javaee.51doit.cn/laowang
object Test2 {
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
    //这里是两次shuffle,聚合操作和重新分区的操作
    val value: RDD[(String, Iterable[((String, String), Int)])] = initial.reduceByKey(_ + _).groupBy(data => data._1._1)
    //对value值操作,如何排序?转list放到内存中分组取topN,排序take方法取topN
    val result: RDD[(String, List[((String, String), Int)])] = value.mapValues(data => data.toList.sortBy(data => data._2)(Ordering.Int.reverse).take(3))
    result.collect.foreach(println)
    sc.stop()
  }
}
