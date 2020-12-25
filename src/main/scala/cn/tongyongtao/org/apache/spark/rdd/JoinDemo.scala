package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//join的分析
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(List(("spark", 1), ("scala", 2), ("kafka", 4), ("flink", 9), ("spark", 1)))
    val rdd2 = sc.makeRDD(List(("spark", 3), ("scala", 4), ("kafka", 5), ("strom", 2), ("strom", 4)))
    //cogroup的效果如:(spark,(CompactBuffer(1, 1),CompactBuffer(3))),考虑使用for循环,
    val cordd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cordd.flatMapValues(data => (data._1)).collect().foreach(println)
    sc.stop()
  }
}
