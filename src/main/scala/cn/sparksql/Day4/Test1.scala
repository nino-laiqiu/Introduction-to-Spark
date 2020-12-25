package cn.sparksql.Day4

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//SQL中的集合数据类型和炸裂函数的使用
object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame = spark.read.format("json").load("src/main/resources/jieh.json")
    println(frame.printSchema)
    spark.stop()
  }
}
