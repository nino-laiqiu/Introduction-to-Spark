package cn.tongyongtao.org.apache.spark.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object MapDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("mapfliter").setMaster("local[*]"))
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    //map算子每个数据都要调用一次效率低下
    rdd.map(data => {
      val parid = TaskContext.get().partitionId()
      val stid = TaskContext.get().stageId()
      (parid, stid, data * 10)
    }).saveAsTextFile("maptest")
    sc.stop()
  }
}
