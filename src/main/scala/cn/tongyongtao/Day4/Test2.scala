package cn.tongyongtao.Day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test2").setMaster("local[*]"))
    val rdd = sc.makeRDD(List[String]("hadoop,yarn,mapreduce", "java,scala,node.js"), 2)
    val flatrdd: RDD[String] = rdd.flatMap(data => data.split(","))
    //key是 charat(0)
    //  val result: RDD[(Char, Iterable[String])] = flatrdd.groupBy(data => data.charAt(0))
    //key是true和false
    val res: RDD[(Boolean, Iterable[String])] = flatrdd.groupBy(data => data.contains("s"))
    res.collect.foreach(println)
    sc.stop()
  }
}
