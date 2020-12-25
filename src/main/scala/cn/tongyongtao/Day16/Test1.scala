package cn.tongyongtao.Day16

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]"))
    val rdd = sc.textFile(args(0))
    val maprdd: RDD[(String, Int)] = rdd.map(data => {
      val txt = data.split(",")
      (txt(0), txt(1).toInt)
    })

    //TODO 获取分区的标准,例如按照姓名来分区,首先触发一次job获取有多少个人
    val groupcount: Array[String] = maprdd.map(_._1).collect()
    val partition = new MyPartition(groupcount)
    //val rerdd: RDD[(String, Int)] = maprdd.repartitionAndSortWithinPartitions(partition)
    implicit val sort = Ordering[String].on[String](t => t).reverse
    val shuffrdd = new ShuffledRDD[String, Int, Int](maprdd, partition)
    shuffrdd.setKeyOrdering(sort)
    shuffrdd.saveAsTextFile(args(1))
    Thread.sleep(Int.MaxValue)
    sc.stop()
  }
}

class MyPartition(groupcount: Array[String]) extends Partitioner {
  private val map = mutable.Map[String, Int]()
  var index = 0
  for (name <- groupcount) {
    map(name) = index
    index += 1
  }

  override def numPartitions: Int = groupcount.length
  //获取的是key
  override def getPartition(key: Any): Int = {
    val value = key.asInstanceOf[String]
    map(value)
  }
}
