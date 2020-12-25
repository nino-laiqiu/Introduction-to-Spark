package cn.tongyongtao.Day5

import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test4").setMaster("local[4]"))
    val rdd = sc.makeRDD(List(("a", 2), ("b", 4), ("b", 1), ("a", 2), ("a", 8), ("c", 1)), 2)
    rdd.reduceByKey(_ + _).saveAsTextFile("src/aa")
    //(a,12)
    //(c,1)
    //(b,5)
    rdd.aggregateByKey(0)(
      (x, y) => Math.max(x, y), (x, x1) => x + x1
    ).saveAsTextFile("src/bb")
    //(a,10)
    //(c,1)
    //(b,4)
    rdd.foldByKey(0)(_+_).saveAsTextFile("/src/cc")
    sc.stop()
  }
}
