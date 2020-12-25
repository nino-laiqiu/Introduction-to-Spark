package cn.tongyongtao.Day13

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//连续登陆案例,工具类的使用
object Test4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)
    if (args(0).toBoolean) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(1))
    val maprdd: RDD[(String, String)] = rdd.map(data => {
      val txt = data.split(",")
      val people = txt(0)
      val date = txt(1)
      (people, date)
    })
    //我要排序,直接sort是全局排序,使用mapvalue
    //规范化数据,求取减去index的日期
    //flatMapValues扁平化,每个value值与key结合
    val value: RDD[(String, (String, String))] = maprdd.groupByKey().flatMapValues(datas => {
      //在组内进行排序
      val disduplicationList = datas.toSet.toList.sorted
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()
      //设置一个index用来记录
      var index = 0
      disduplicationList.map(data => {
        val date: Date = sdf.parse(data)
        //设置日期
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -index)
        index += 1
        (data, sdf.format(calendar.getTime))
      })
    })
    value.map(data => {
      ((data._1, data._2._2), data._2._1)
    }).groupByKey().collect().foreach(println)

    val result: RDD[(String, (Int, String, String))] = value.map(data => {
      ((data._1, data._2._2), data._2._1)
    }).groupByKey().mapValues(data => {
      //这里可以使用reducebykey获取最大值 最小值 和 sum和
      data.toList.sorted
      val size = data.size
      val head = data.head
      val last = data.last
      (size, head, last)
    }).filter(data => data._2._1 >= 3).map(data => (data._1._1, data._2))
    result.collect().foreach(println)
    sc.stop()
  }
}

//晚上进一步优化算子
