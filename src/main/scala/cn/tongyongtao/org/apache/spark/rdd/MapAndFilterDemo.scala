package org.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object MapAndFilterDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("mapfliter").setMaster("local[*]"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val map: MapPartitionsRDD[Double, Int] = new MapPartitionsRDD[Double,Int](rdd, (_, _, iter) => iter.map(data => {
      data.toString.toDouble * 10
    }))
    val filter = new MapPartitionsRDD[Double, Double](map, (_, _, iter) => iter.filter(data => data > 4))
    filter.collect().foreach(println)
    sc.stop()
  }
}
