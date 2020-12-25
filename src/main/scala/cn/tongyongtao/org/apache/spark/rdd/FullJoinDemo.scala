package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object FullJoinDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("groupbykey").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(List(("spark", 1), ("scala", 2), ("kafka", 4), ("flink", 9), ("spark", 1)))
    val rdd2 = sc.makeRDD(List(("spark", 3), ("scala", 4), ("kafka", 5), ("strom", 2), ("strom", 4)))
    val cprdd = rdd1.cogroup(rdd2)
    cprdd.flatMapValues {
      case (vs, Seq()) => vs.iterator.map((_,None))
      case (Seq(), ws) => ws.iterator.map((None,_))
      case (vs, ws) => for (it <- vs.iterator; it2 <- ws.iterator) yield (Some(it), Some(it2))
    }.collect().foreach(println)
    sc.stop()
  }
}
