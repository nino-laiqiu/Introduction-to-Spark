package cn.tongyongtao.Day12

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

//排序
object Test2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sort").setMaster("local[*]"))
    val json = List("{\"name\":\"小妮\",\"age\":\"18\",\"salary\":18}","{\"name\":\"小化\",\"age\":\"22\",\"salary\":12}")
    val jsontxt = sc.makeRDD(json)
    jsontxt.map(data => {
      val teacher = JSON.parseObject(data, classOf[Teacher])
      teacher
    }).collect().foreach(println)
    sc.stop()
  }
}
