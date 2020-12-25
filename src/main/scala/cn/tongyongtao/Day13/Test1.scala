package cn.tongyongtao.Day13

import org.apache.spark.{SparkConf, SparkContext}

object  Test1 {
def main(args: Array[String]): Unit = {
new SparkContext(new SparkConf().setMaster("local[1]").setAppName("wc"))
  .textFile("src/main/resources/dm.txt").flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
        .sortBy(_._2)
        .collect()
          Thread.sleep(Long.MaxValue)
    //.textFile("src/main/resources/dm.txt").saveAsTextFile("scr/bb")
    }
  }
