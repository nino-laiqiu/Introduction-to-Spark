package cn.tongyongtao.Day15

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//数据如下类型,需求获取每个月份的总金额并且迭代增加
//1,2020-01-02,20
//1,2020-01-03,20
//1,2020-01-04,20
object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args(0).toBoolean) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //数据的结构: ((1,2020-1),20)
    val rdd = sc.textFile(args(1))
    val maprdd = rdd.map(data => {
      val txt = data.split(",")

      val shopid = txt(0)
      val date = txt(1).split("-")
      val turnover = txt(2).toInt
      ((shopid, (date(0) + "-" + date(1))), turnover)
    })
    val reducebyrdd: RDD[((String, String), Int)] = maprdd.reduceByKey(_ + _)
    //是否可以避免掉??
    val re = reducebyrdd.map({
      case ((x, y), z) => ((x), (y, z))
    }).groupByKey().map(data => {
      //使用一个变量来接收一个累加值
      val sorted: List[(String, Int)] = data._2.toList.sorted
      var sum = 0
      sorted.map(num => {
        sum += num._2
        (data._1, (num._1, sum))
      })
    })
    re.collect().foreach(println)
    sc.stop()
  }
}
