package cn.tongyongtao.Day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//需求：统计出每一个省份广告被点击次数的TOP3
//数据的示例:
//1516609143869 4 6 1 11
//1516609143869 3 6 49 7
//1516609143869 8 3 4 18
//1516609143869 8 8 69 14
//1516609143869 0 6 51 29
//1516609143869 5 3 59 2
//1516609143869 8 4 66 25

//需求:时间戳 省份 用户 城市 广告
object Test7 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test7").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/top.txt")
    //由于shuffle传输的字节越多性能越差,只需要需求的数据
    val maprdd: RDD[((String, String), Int)] = rdd.map(data => {
      val th = data.split(" ")
      //结构为 ((省份,城市),广告数)
      ((th(1), th(3)), th(4).toInt)
    })
    //使用效率高的reducebykey,求取出(省份,城市)分组内的所有广告数
    val reducerdd: RDD[((String, String), Int)] = maprdd.reduceByKey(_ + _)
    //使用case 模式匹配重新分组
    val mapardd = reducerdd.map(data => data match {
      case ((province, city), add) => (province, (city, add))
    })
    val grouprdd: RDD[(String, Iterable[(String, Int)])] = mapardd.groupByKey()
    //获取前三
    val result: RDD[(String, List[(String, Int)])] = grouprdd.mapValues(data => data.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    result.collect.foreach(println)
    sc.stop()
  }
}
