package cn.tongyongtao.Day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    // def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)
    //其中的U是初始值 U是传递过来的值
    //求字母key的平均值
    var sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val list = List(("a", 2), ("b", 3), ("a", 4), ("b", 1), ("c", 8), ("a", 3))
    val rdd = sc.makeRDD(list)
    //第一个0叠加总数 第二个0叠加次数
    //类型(Int, Int)与传入的(0,0)是一致的
    val value: RDD[(String, (Int, Int))] = rdd.combineByKey(
      //在分区内进行叠加
      //V ->  U 的转换
      v => (v,1),
      // U 的类型是(v,1)
      (u:(Int,Int), v) => (u._1 + v, u._2 + 1),
      //x是初始值和计算后的结果,x1是要叠加的值
      (x:(Int,Int), x1) => (x._1 + x1._1, x._2 + x1._2)
    )
    //value值的第一个是总和,第二个值个数
    //方法一mapvalue
    val result: RDD[(String, Int)] = value.mapValues(data => data match {
      case (x, y) => x / y
    })
    //方法二
    val result1: RDD[(String, Int)] = value.map(data => (data._1, (data._2._1 / data._2._2)))
    result1.collect.foreach(println)
    sc.stop()
  }
}
