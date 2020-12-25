package cn.tongyongtao.Day6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1").setMaster("local[*]"))
    val data = sc.textFile("src/main/resources/sanguo.txt")
    //首先求取总成绩大于150的姓名和总成绩
    val mapdd = data.map(txt => {
      val strings = txt.split(" ")
      (strings(1), (strings(4),strings(5).toInt))
    })
    //获取总成绩和科目数
    val aggrdd: RDD[(String, (Int, Int))] = mapdd.aggregateByKey((0, 0))(
      (u, v) => ((u._1 + v._2), u._2 + 1),
      (x, x1) => ((x._1 + x1._1), x._2 + x1._2)
    )
    //得到的是总成绩大于150的姓名和平均值
    val re1: RDD[(String, Int)] = aggrdd.filter(_._2._1 > 150).mapValues(data => data._1 / data._2)
    //(王英,73)
    //(宋江,60)
    //(杨春,70)
    //(李逵,63)
    //(林冲,53)

    //且数学大于等于70，且年龄大于等于19岁的学生
    val maprdd: RDD[Array[String]] = data.map(_.split(" "))
    val filtrdd: RDD[Array[String]] = maprdd.filter(data => data(2).toInt >= 19 && data(4).equals("math") && data(5).toInt >= 70)
    val re2: RDD[(String, Int)] = filtrdd.map(data => {
      (data(1), data(5).toInt)
    })
    //join连接
    val result: RDD[(String, Int)] = re1.join(re2).map(data => {
      (data._1, data._2._1)
    })
    result.collect.foreach(println)
    sc.stop()
    //(王英,73)
    //(杨春,70)
  }
}
