package org.apache.spark

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val maprdd = sc.makeRDD(List("spark", "scala", "java", "js", "flink",
      "spark", "scala", "java", "js", "flink",
      "spark", "scala", "java", "js", "flink")).map(data => (data, 1))

    val shufflerdd = new ShuffledRDD[String, Int, CompactBuffer[Int]](maprdd, new HashPartitioner(maprdd.partitions.length))
    //设置局部不聚合
    shufflerdd.setMapSideCombine(false)
    //将每个组内的元素的value值放到集合中
    val createCombiner = (v: Int) => CompactBuffer(v)
    //把组内其他元素添加到集合中
    val mergeValue = (buf: CompactBuffer[Int], v: Int) => buf += v
    //在下游进行合并
    val mergeCombiners = (c1: CompactBuffer[Int], c2: CompactBuffer[Int]) => c1 ++= c2
    val groupbukeyrdd = shufflerdd.setAggregator(new Aggregator[String, Int, CompactBuffer[Int]](
      createCombiner, mergeValue, mergeCombiners
    ))
    groupbukeyrdd.collect().foreach(println)
    sc.stop()
  }
}
