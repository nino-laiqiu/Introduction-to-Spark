package cn.tongyongtao.Day2

object Test1 {
  def main(args: Array[String]): Unit = {
        var list  = List[String]("a,b,c,d","e,f,g")
        val strings = list.flatMap(_.split(","))
        //先进行map 进行对每一个部分"," 切分,之后进行扁平化
        list.map(_.split(",")).foreach(println)  //List(Array)类型  地址值
       val stringses = list.map(_.split(","))
       for (index <- stringses){
            println(index.mkString(","))
       }
       list.flatten.foreach(println)//包含","没有进行切分
  }
}
