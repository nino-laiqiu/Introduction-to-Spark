package cn.tongyongtao.Day3

import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test5").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD = sc.makeRDD(List[Int](1, 2, 3, 4, 5))
    val result = RDD.mapPartitionsWithIndex((index, iter) => {
      iter.map(data => {
        (index, data)
      }
      )
    })
    result.collect.foreach(println)
    sc.stop()
  }
}
