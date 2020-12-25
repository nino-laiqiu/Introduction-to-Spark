package cn.sparksql.Day2


import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object Spark_Udaf2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame = spark.read.json("src/main/resources/1.json")
    frame.createOrReplaceTempView("user")
    spark.udf.register("myavg", functions.udaf(new MyAvg1))
    spark.sql("select myavg(age) from user").show()
    spark.stop()
  }
}
case class Buff(var total: Long, var count: Long)

class MyAvg1 extends Aggregator[Long, Buff, Long] {
  override def zero: Buff = {
    Buff(0L, 0L)
  }
  //根据输入的数据更新缓冲区数据
  override def reduce(buff: Buff, input: Long): Buff = {
    buff.total = buff.total + input
    buff.count = buff.count + 1
    buff
  }
  //合并多个缓冲区数据
  override def merge(buff1: Buff, buff2: Buff): Buff = {
    buff1.total = buff1.total + buff2.total
    buff1.count = buff1.count + buff2.count
    buff1
  }
  //输出的结果
  override def finish(reduction: Buff): Long = {
    reduction.total / reduction.count
  }
  //下面的两个是序列化
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
