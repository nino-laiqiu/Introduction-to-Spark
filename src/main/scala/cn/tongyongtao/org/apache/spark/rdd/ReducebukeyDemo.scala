package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object ReducebukeyDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val maprdd = sc.makeRDD(List("spark", "scala", "java", "js", "flink",
      "spark", "scala", "java", "js", "flink",
      "spark", "scala", "java", "js", "flink"), 2).map(data => (data, 1))
    val createCombiner = (x: Int) => x
    val mergeValue = (x: Int, y: Int) => x + y
    val mergeCombiners = (x1: Int, x2: Int) => x1 + x2
    maprdd.combineByKey(
      (x => x), (x: Int, y: Int) => x + y, (x1: Int, x2: Int) => x1 + x2
    ).saveAsTextFile("combinebykey")
    //使用shuffleRDD
    val shufflerdd = new ShuffledRDD[String, Int, Int](maprdd, new HashPartitioner(maprdd.partitions.length))
    //是否局部聚合
    shufflerdd.setMapSideCombine(true)
    shufflerdd.setAggregator(new Aggregator[String, Int, Int](
      createCombiner, mergeValue, mergeCombiners
    )).saveAsTextFile("shufflerdd")
    sc.stop()
  }
}
