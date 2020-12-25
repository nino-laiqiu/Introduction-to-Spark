package cn.tongyongtao.Day15

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FlowRollupCount {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if (isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(1))

    //1,2020-02-18 14:20:30,2020-02-18 14:46:30,20
    val uidAndTimeFlow: RDD[(String, (Long, Long, Double))] = lines.mapPartitions(it => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      it.map(line => {
        val fields = line.split(",")
        val uid = fields(0)
        val startTime = fields(1)
        val startTimestamp = sdf.parse(startTime).getTime
        val endTime = fields(2)
        val endTimestamp = sdf.parse(endTime).getTime
        val downFlow = fields(3).toDouble
        (uid, (startTimestamp, endTimestamp, downFlow))
      })
    })

    val uidAndTimeFlowFlag: RDD[(String, (Long, Long, Double, Int))] = uidAndTimeFlow.groupByKey().flatMapValues(it => {
      var temp = 0L
      var flag = 0 //要么等于0， 要么等于1
      var sum = 0  //对flag的 累加
      //按照starttime来排序
      val sorted: List[(Long, Long, Double)] = it.toList.sortBy(_._1)
      sorted.map(t => {
        val startTimestamp = t._1
        val endTimestamp = t._2
        if (temp != 0L) {
          if ((startTimestamp - temp) / 60000 > 10) {
            flag = 1
          } else {
            flag = 0
          }
        }
        sum += flag
        temp = endTimestamp
        (startTimestamp, endTimestamp, t._3, sum)
      })
    })

    val res = uidAndTimeFlowFlag.map{
      case(uid, (start, end, flow, flag)) => ((uid, flag), (start, end, flow))
    }.reduceByKey((t1, t2) => {
      (Math.min(t1._1, t2._1), Math.max(t1._2, t2._2), t1._3 + t2._3)
    }).mapPartitions(it => {
      val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      it.map{
        case(uid, (start, end, flow)) => {
          val statTime = sdf.format(start)
          val endTime = sdf.format(end)
          (uid, statTime, endTime, flow)
        }
      }
    })

   res.collect().foreach(println)

  }
}
