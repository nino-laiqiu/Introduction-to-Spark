package cn.tongyongtao.Day11

import scala.collection.mutable

object Test1 {
  def main(args: Array[String]): Unit = {
    implicit  val sort =Ordering[Int].on[((String, String), Int)](t => t._2)
    val tuples = mutable.TreeSet[((String, String), Int)]()
    tuples.add((("小明","小华"),2))
    tuples.add((("小明","小华1"),2))
    tuples.add((("小明","小华1"),5))
    tuples.add((("小明","小华"),2))
    tuples.add((("小明","小华2"),5))
    println(tuples)

  }
}
