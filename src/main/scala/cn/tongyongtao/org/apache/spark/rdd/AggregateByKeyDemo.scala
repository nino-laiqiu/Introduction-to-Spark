package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val maprdd = sc.makeRDD(List("spark", "spark", "scala", "js", "flink",
      "scala", "scala", "java", "js", "flink",
    ), 5).map(data => (data, 1))
    val shuffrdd = new ShuffledRDD[String, Int, Int](maprdd, new HashPartitioner(maprdd.partitions.length))
    shuffrdd.setMapSideCombine(true)
    val f1= (x:Int) => x + 0
    val f2 = (x: Int, y: Int) => x + y
    val f3 = (x1: Int, x2: Int) => x1 + x2
    shuffrdd.setAggregator(new Aggregator[String, Int, Int](
         f1,f2,f3
    )).saveAsTextFile("agg")
    sc.stop()
  }
}
