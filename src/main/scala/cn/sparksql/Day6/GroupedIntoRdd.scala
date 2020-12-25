package cn.sparksql.Day6

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//分组转化案例
object GroupedIntoRdd {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/data.csv")
    val maprdd = rdd.map(line => {
      val data = line.split(",")
      (data(0).toInt, (data(1), data(2), data(3).toInt))
    })

    //按id来分组处理
    val etlrdd = maprdd.groupByKey().flatMapValues(data => {
      //下一条的begin_time - 上一条的end_time > 10min再分一组
      //按照begin_time来排序
      val sort = data.toSet.toList.sorted
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      var flag = 0 // 0或者1 用来判断是否小于十分钟
      var sum = 0 //划分分组
      var temp_time = 0L //接收上一个的end_time
      sort.map(da => {
        val begin_time = sdf.parse(da._1).getTime
        val end_time = sdf.parse(da._2).getTime
        if (temp_time != 0L) {
          if ((begin_time - temp_time) / (1000 * 60) < 10) {
            flag = 0
          }
          else {
            flag = 1
          }
        }
        sum += flag
        temp_time = end_time
        (begin_time, end_time, da._3, sum)
      })
    }).map({ case (x, (y, z, q, p)) => ((x, p), (y, z, q)) })

    val value = etlrdd.reduceByKey((x, y) => {
      val min = Math.min(x._2, y._2)
      val max = Math.max(x._2, y._2)
      var sum = x._3 + y._3
      (min, max, sum)
    }
    ).mapPartitions(iter => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      iter.map(data => {
        val start = sdf.format(data._2._1)
        val last = sdf.format(data._2._2)
        ((data._1._1, start), (last, data._2._3))
      })
    }).sortByKey()
    value.collect().foreach(println)
    sc.stop()
  }
}
