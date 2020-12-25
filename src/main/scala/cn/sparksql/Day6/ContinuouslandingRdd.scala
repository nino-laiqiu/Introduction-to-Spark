package cn.sparksql.Day6

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object ContinuouslandingRdd {

  //p1,2020-12-1
  //p1,2020-12-2
  //p1,2020-12-3
  //p1,2020-12-4
  //p1,2020-12-6
  //p1,2020-12-7
  //p1,2020-12-11
  //p1,2020-12-13
  //p2,2020-12-01
  //p2,2020-12-13
  //p2,2020-12-13
  //p2,2020-12-14
  //p2,2020-12-15
  //p3,2020-12-15
  //p4,2020-12-15


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))

    val rdd = sc.textFile("src/main/resources/date1.csv")
    val maprdd: RDD[(String, String)] = rdd.map(line => {
      val data = line.split(",")
      (data(0), data(1))
    })
    //思路groupbykey获取去重,按照时间戳排序的组,减去排序的序列化,如果相等就是连续登陆的
    val etlrdd: RDD[(String, (String, String))] = maprdd.groupByKey().flatMapValues(datas => {
      //去重按照时间戳来排序
      val sort = datas.toSet.toList.sorted
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()
      var index = 0
      sort.map(data => {
        val date: Date = sdf.parse(data)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -index)
        index += 1
        (data, sdf.format(calendar.getTime))
      })
    })

    //获取连续登陆的开始时间,结束时间,以及练习登陆的天数
   etlrdd.map ({
      case (x, (y, z)) => ((x, z), y)
    }).groupByKey().mapValues(data => {
     val sort = data.toList.sorted
     val first_time = sort.head
     val last_time = sort.last
     val lon = sort.size
     (lon,first_time,last_time)
   }).filter(_._2._1 >= 3).map({case((x,y),(z,q,p)) => (x,q,p) }).collect().foreach(println)

    //(p2,2020-12-13,2020-12-15)
    //(p1,2020-12-2,2020-12-4)

  }
}
