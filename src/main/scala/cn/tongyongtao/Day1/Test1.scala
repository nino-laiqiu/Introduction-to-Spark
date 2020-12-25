package cn.tongyongtao.Day1

import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/aa.txt",2)
      data.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .saveAsTextFile("src/main/aa")


      sc.stop()
  }
}
