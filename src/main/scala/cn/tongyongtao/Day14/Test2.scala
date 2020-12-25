package cn.tongyongtao.Day14

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//形如如下数据,同一个id的末尾时间-初始时间大于10min就再分一组
//3,2020/2/18 14:39,2020/2/18 15:35,20
//3,2020/2/18 15:36,2020/2/18 15:24,30
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("流量统计")
    val boolean = args(0).toBoolean
    if (boolean) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(1))
    val maprdd: RDD[(String, (String, String, String))] = rdd.map(data => {
      val txt = data.split(",")
      val id = txt(0)
      val starttime = txt(1)
      val lasttime = txt(2)
      val money = txt(3)
      (id, (starttime, lasttime, money))
    })
    //由于要对同一个id进行操作,考虑把对id进行分组
    val groupbyrdd = maprdd.groupByKey()
    val value: RDD[(String, (String, String, Int, Long))] = groupbyrdd.flatMapValues(data => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm")
   //   val parstime = Calendar.getInstance()
       data.map(txt => {
        val start: Date = sdf.parse(txt._1)
        val last: Date = sdf.parse(txt._2)
        //怎么进行两个日期相减?
        //获取两个日期之间相差多少秒
        val l = (last.getTime - start.getTime) / (60 * 1000)
        //如果日期相减是小于10min的我就放在集合中,只要有一个日期相减大于10,就把存入的数据全部取出来??
        if (l < 10) {
          (txt._1, txt._2, txt._3.toInt, 0L)
        }
        else {
          //由于l可能会重复,所以我采用时间戳来表示
          ((txt._1, txt._2, txt._3.toInt,l ))
        }
      })
    })
    value.map(data => {
      ((data._1,data._2._4),(data._2._1,data._2._2,data._2._3))
    }).reduceByKey((x,y)=>{
          var sum =  x._3 + y._3
          var min = if (x._1 > y._1)  y._1 else x._1
          var max  = if (x._2 > y._2)  x._2   else y._2
      (min,max,sum)
    })
    value.collect().foreach(println)
    sc.stop()
  }
}
