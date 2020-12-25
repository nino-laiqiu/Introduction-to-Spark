package cn.tongyongtao.Day12

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object Test6 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sort").setMaster("local[*]"))
    val json = List("{\"name\":\"小妮\",\"age\":\"18\",\"salary\":18}", "{\"name\":\"小化\",\"age\":\"22\",\"salary\":12}")
    val jsontxt = sc.makeRDD(json)
    val mapjson = jsontxt.map(data => {
      val employee = JSON.parseObject(data, classOf[Employee])
      employee
    })
    import  ObjectContext.order
    mapjson.sortBy(e => e).collect().foreach(println)
    sc.stop()
  }
}
