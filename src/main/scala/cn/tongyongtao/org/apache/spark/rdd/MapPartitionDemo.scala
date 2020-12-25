package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object MapPartitionDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("mapfliter").setMaster("local[*]"))
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    rdd.mapPartitions(it => {
      println("******")
      val parid = TaskContext.getPartitionId()
      //这里的map方法是对迭代器中每个数据进行操作
      val tuples: Iterator[(Int, Int)] = it.map(data => {
        (parid, data * 10)
      })
      tuples
    }).saveAsTextFile("mappartition")
  }
}
