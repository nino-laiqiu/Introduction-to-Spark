package cn.tongyongtao.Day13

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("iptest").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/ip.txt", 4)
    val iptxt: Array[(Long, Long, String, String)] = rdd.map(data => {
      val txt = data.split("[|]")
      val start = txt(2).toLong
      val stop = txt(3).toLong
      val province = txt(6)
      val city = txt(7)
      (start, stop, province, city)
    }).collect()
    //广播变量,这是一个阻塞方法,没有广播完,下面的代码无法执行
    val ipBT: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(iptxt)
    val IAP: RDD[(String, Int)] = sc.textFile("src/main/resources/ipaccess.log").map(data => {
      val logtxt = data.split("[|]")
      //转换编码
      val l = IpUtils.ip2Long(logtxt(1))
      //在广播数据中查找,知识点二分查找
      val executorindex = IpUtils.binarySearch(ipBT.value, l)
      var province: String = null
      if (executorindex != -1) {
        //说明查找到了数据
        province = ipBT.value(executorindex)._3
      }
      //wordcount案例
      (province, 1)
    })
    IAP.map {
      case (x, y) => (x, y + 10)
    }.reduceByKey(_ + _).collect().foreach(println)
    sc.stop()
  }
}
