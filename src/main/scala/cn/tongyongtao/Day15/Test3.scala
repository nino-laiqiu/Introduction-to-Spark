package cn.tongyongtao.Day15

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

//2,2020/2/18 16:32,2020/2/18 16:40,40
//2,2020/2/18 16:44,2020/2/18 17:40,50
//3,2020/2/18 14:39,2020/2/18 15:35,20
//3,2020/2/18 15:36,2020/2/18 15:55,30
//数据已经排序过
object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (args(0).toBoolean) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(1))
    //首先把数据格式化
    val groupbykeyrdd = rdd.mapPartitions(txt => {
      txt.map(data => {
        val traffic = data.split(",")
        val id = traffic(0)
        val starttime = traffic(1)
        val lasttime = traffic(2)
        val flux = traffic(3).toInt
        (id, (starttime, lasttime, flux))
      })
    }).groupByKey()

    val flatrdd: RDD[((String, Int), (Long, Long, Int))] = groupbykeyrdd.flatMapValues(data => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      //考虑使用一个temp变量来接收上一个lasttime
      //考虑使用flag变量来作为标签,如果时间戳相减<10min就为0 否则就为1
      //考虑使用一个sum变量来作为分组的标准
      val sorted = data.toList.sorted
      var temp = 0L
      var flag = 0
      var sum = 0
      sorted.map(txt => {
        val startdate = sdf.parse(txt._1).getTime
        val lastdate = sdf.parse(txt._2).getTime
        //怎么处理第一个值
        if (temp != 0L) {
          if ((startdate - temp) / (60 * 1000) < 10) {
            flag = 0
          }
          else {
            flag = 1
          }
        }
        sum += flag
        temp = lastdate
        (startdate, lastdate, txt._3, sum)
      })
    }).map {
      case (x, (y, z, p, q)) => ((x, q), (y, z, p))
    }
    val mypartition = new Mypartition(flatrdd.partitions.length)
     //这里我要自定义分区器,防止发生乱序
    val value: RDD[((String, Int), (Long, Long, Int))] = flatrdd.reduceByKey(mypartition,(x, y) => {
      var firsttime = Math.min(x._1, y._1)
      var lasttime = Ordering[Long].max(x._2, y._2)
      var sum = x._3 + y._3
      (firsttime, lasttime, sum)
    })

  val result=  value.mapPartitions(txt => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      txt.map(data  => {
        val time1: String = sdf.format(data._2._1)
        val time2: String = sdf.format(data._2._2)
        (data._1._1,time1,time2,data._2._3)
      })
    })//.map(data => ((data._1,data._2),(data._3,data._4))).sortByKey()
    result.saveAsTextFile("ppp")

    sc.stop()
  }
}

class  Mypartition(partitonlengh: Int) extends  Partitioner{
  override def numPartitions: Int =  partitonlengh
  override def getPartition(key: Any): Int = {
        key.asInstanceOf[((String, Int))]._1.toInt  / partitonlengh
  }
}
