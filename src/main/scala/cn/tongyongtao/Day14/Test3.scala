package cn.tongyongtao.Day14

import java.util.Date

object Test3 {
  def main(args: Array[String]): Unit = {
    val date = new Date()
    println((date.getTime - 1607245701098L) /(60 * 1000)  )
  }
}
