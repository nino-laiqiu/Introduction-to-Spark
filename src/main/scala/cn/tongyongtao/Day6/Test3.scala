package cn.tongyongtao.Day6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
////22. 总成绩大于150分的12班的女生有几个？
////23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/sanguo.txt")
    val filtrdd: RDD[Array[String]] = rdd.map(_.split(" ")).filter(data => data(0).equals("12") && data(3).equals("女"))
    val rerdd: RDD[(String, Int)] = filtrdd.map(data => (data(1), data(5).toInt)).reduceByKey(_ + _)
    rerdd.collect.foreach(println)
    val result: Long = rerdd.filter(_._2 > 150).count()
    println(result)
    sc.stop()
  }
}
