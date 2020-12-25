package cn.sparksql.Day5

import ch.hsr.geohash.GeoHash

object Test2 {
  def main(args: Array[String]): Unit = {
    val geo: String = GeoHash.withCharacterPrecision(34.22368071,116.1574537, 10).toBase32
    val str = GeoHash.withCharacterPrecision(45.667, 160.876547, 6).toBase32()
    println(geo)
    println(str)
  }
}
