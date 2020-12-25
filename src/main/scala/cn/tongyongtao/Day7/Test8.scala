package cn.tongyongtao.Day7

import org.apache.spark.{SparkConf, SparkContext}

object Test8 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("test5").setMaster("local[*]"))
    var  rdd =sc.makeRDD(List(("A",11),("B",11),("C",11),("A",111)),2)
    //初始值参与每个区内的每个组的运算,
    //组的粒度的聚合
   rdd.aggregateByKey(10)(_+_,_+_).collect.foreach(println)
    //初始值参与分区内和分区间
    //全局聚合
    val result: Int = rdd.aggregate(10)((x, y) => x + y._2, (x1, x2) => x1 + x2)
    println(result)
    sc.stop()
  }
}
