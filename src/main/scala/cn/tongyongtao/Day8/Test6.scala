package cn.tongyongtao.Day8

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Test6 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test6").setMaster("local[*]"))
    val rdd = sc.makeRDD(List("scala", "spark", "scala", "flink"), 2)
    val myrdd = rdd.map(data => (data, 1)).partitionBy(new MyPartitioner)
    myrdd.saveAsTextFile("src/bbb")
    sc.stop()

  }
}

class MyPartitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = {
    key match {
      case "scala" => 1
      case _ => 0
    }
  }
}