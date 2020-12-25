package cn.tongyongtao.Day17

import java.text.SimpleDateFormat

object DateUntil {
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parse(st: String): Long = synchronized {
    val date = format.parse(st)
    date.getTime
  }
}
