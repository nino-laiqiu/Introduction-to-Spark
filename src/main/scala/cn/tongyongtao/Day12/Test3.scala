package cn.tongyongtao.Day12

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sort").setMaster("local[*]"))
    val json = List("{\"name\":\"小妮\",\"age\":\"18\",\"salary\":18}", "{\"name\":\"小化\",\"age\":\"22\",\"salary\":12}")
    val jsontxt = sc.makeRDD(json)
    val value = jsontxt.map(data => {
      JSON.parseObject(data, classOf[People])
    })

   //value.sortBy(p => p).collect.foreach(println)
    sc.stop()
  }
}
