package cn.tongyongtao.Day4

import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test3").setMaster("local[*]"))
    val RDD = sc.makeRDD(List[Int](1, 2, 3, 4, 1, 4, 7, 8, 9),3)
    RDD.coalesce(2,true).saveAsTextFile("src/aa")
    sc.stop()



}
}
