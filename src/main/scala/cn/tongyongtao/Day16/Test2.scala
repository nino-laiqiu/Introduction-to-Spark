package cn.tongyongtao.Day16

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val nPartition = new MYpartition(1)
    val rdd = sc.makeRDD(List((1, 2), (1, 3)),2)
    val rdd1 = sc.makeRDD(List( (1, 4), (1, 5)),2)
    val maprdd1 = rdd.map(data => data)
    val maprdd2 = rdd1.map(data => data)
    val rerdd1 = maprdd1.reduceByKey(new HashPartitioner(rdd.partitions.length), _ + _)
    val rerdd2 = maprdd2.reduceByKey(new HashPartitioner(rdd.partitions.length), _ + _)

    rerdd1.cogroup(rerdd2).saveAsTextFile("rr")

   //  val value1 = value.map(data => (data._1, data._2._1)).groupByKey()
   // value1.collect().foreach(println)
    Thread.sleep(Int.MaxValue)
    sc.stop()
  }
}

class MYpartition(var num:Int) extends Partitioner {

  override def numPartitions: Int = 1

  override def getPartition(key: Any): Int = {
             1
  }
}
