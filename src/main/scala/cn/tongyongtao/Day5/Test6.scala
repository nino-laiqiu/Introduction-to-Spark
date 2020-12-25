package cn.tongyongtao.Day5

import org.apache.spark.{SparkConf, SparkContext}

object Test6 {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val list = List(("a", 2), ("b", 3), ("a", 4), ("b", 1), ("c", 8), ("a", 3))
    val rdd = sc.makeRDD(list)
    //四种聚合算子
    rdd.reduceByKey(_+_).collect.foreach(println)
    rdd.aggregateByKey(0)((x,y)=>x+y,(x,x1)=>x+x1).collect.foreach(println)
    //foldByKey算子是对aggregateByKey分区内操作的简化版
    rdd.foldByKey(0)(_+_).collect.foreach(println)
    //combineByKey算子是aggregateByKey第一个参数的简化
    rdd.combineByKey(x=>x,(x:Int,y)=>x+y,(x:Int,x1:Int)=>x+x1).collect.foreach(println)
    sc.stop()
  }
}
