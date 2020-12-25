package cn.tongyongtao.Day12

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sort").setMaster("local[*]"))
    val json = List("{\"name\":\"小妮\",\"age\":\"18\",\"salary\":18}","{\"name\":\"小化\",\"age\":\"22\",\"salary\":12}")
    val jsontxt = sc.makeRDD(json)
    jsontxt.map(data => {
      val nObject = JSON.parseObject(data)
      val name = nObject.getString("name")
      val age = nObject.getInteger("age").toInt
      val salary = nObject.getDouble("salary")
      (salary,(age,name))
    }).sortBy(t => t).collect().foreach(println)
    sc.stop()
  }
}
