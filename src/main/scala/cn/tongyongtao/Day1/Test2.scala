package cn.tongyongtao.Day1

object Test2 {
  def main(args: Array[String]): Unit = {
    val array = Array[String]("sdff fsewfgsgfsg ggg", "etwf wet tgwt tgwg")
    //任然时候数组形式存在
    val stringses: Array[Array[String]] = array.map(_.split(" "))
    //扁平化操作
    val flatten: Array[Char] = array.flatten
    // println(flatten.toList)
    array.map(_.split(" ")).flatten.toList.foreach(println)

    //等价于flatmap算子
    array.flatMap(_.split(" ")).toList.foreach(println)

    //flatten只进行一次扁平操作
    val stringses1: Array[Array[String]] = array.map(_.split(" "))
    val flatten1: Array[String] = stringses1.flatten
    val flatten2: Array[Char] = flatten1.flatten
    //等价于flatmap算子
    //把全部的array添加到array(array)中
    array.flatMap(data => {
      val strings: Array[String] = data.split(" ")
      println(strings.toList)
      println(strings.mkString(","))
      strings
    }).toList.foreach(println)
  }
}
