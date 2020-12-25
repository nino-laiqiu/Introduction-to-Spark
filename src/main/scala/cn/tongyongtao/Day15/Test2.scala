package cn.tongyongtao.Day15

import java.io
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//数据如下
//1,2020/2/18 15:24,2020/2/18 15:35,40
//1,2020/2/18 16:06,2020/2/18 16:09,50
//1,2020/2/18 17:21,2020/2/18 17:22,60
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args(0).toBoolean) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(1))
    val maprdd = rdd.map(data => {
      val flow = data.split(",")
      val id = flow(0)
      val starttime = flow(1)
      val lasttime = flow(2)
      val money = flow(3).toInt
      (id, (starttime, lasttime, money))
      //这里的分组是必要的吗
    }).groupByKey()

    val flatmaprdd: RDD[(String, (String, String, Int, Long))] = maprdd.flatMapValues(data => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm")
      //定义一个便签值来区分什么时候发生了小于10min的情况
      var number: Long = 0L
      data.map(num => {
        val startdate = sdf.parse(num._1)
        val lastdate = sdf.parse(num._2)
        val differtime = (lastdate.getTime - startdate.getTime) / (60 * 1000)
        if (differtime <= 10) {
          (num._1, num._2, num._3, number)
        }
        else {
          //怎么定义最后一个值,这个值是唯一的
          number += 1L
          (num._1, num._2, num._3, System.currentTimeMillis() + number)
        }
      })
    })
    //重新确定key的值
    val map1rdd = flatmaprdd.map {
      case (x, (y, z, p, q)) => ((x, q), (y, z, p))
    }
    map1rdd.collect().foreach(println)

    //使用reducebykey获取sum , start last
    map1rdd.reduceByKey((x, y) => {
      var sum = x._3.toInt + y._3.toInt
      var first = if (x._1 < y._1) x._1 else y._1
      var last = if (x._2 > y._2) x._2 else y._2
      (first, last, sum)
    }
    ).map(data => (data._1._1, data._2._1, data._2._2, data._2._3)).sortBy(data => data).collect().foreach(println)
    //flatmaprdd.collect().foreach(println)
    Thread.sleep(Int.MaxValue)
    sc.stop()

  }
}

