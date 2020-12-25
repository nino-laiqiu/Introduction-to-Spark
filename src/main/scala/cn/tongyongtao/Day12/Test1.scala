package cn.tongyongtao.Day12

import org.apache.spark.{SparkConf, SparkContext}
//实现排序功能
object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sorted").setMaster("local[*]"))
    val rdd = sc.makeRDD(List("小明,19,130.0", "小华,20,780.0", "小妮,18,230.9"))

    rdd.map(data => {
      val txt = data.split(",")
      val name =txt(0)
      val age = txt(1).toInt
      val salary = txt(2).toDouble
      new Boy(name,age,salary)
    }).sortBy(boy => boy).collect().foreach(println)
    sc.stop()
  }
}
