package cn.tongyongtao.Day8

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    sc.setCheckpointDir("src/ff")
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 1, 2, 3, 4))
    val mardd = rdd.map(data => {
      //每个collect执行了一次,重复操作
      println("*******")
      (data, 1)
    })
    //考虑持久化
    //只能持戒化到内存
    // mardd.cache()
    //可持久化到磁盘
    // mardd.persist(StorageLevel.DISK_ONLY)
    mardd.checkpoint()
    mardd.groupByKey().collect.foreach(println)
    mardd.reduceByKey(_ + _).collect.foreach(println)
    sc.stop()
  }
}
