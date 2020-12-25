package cn.sparksql.Day2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //TODO 读取
    //spark.read.format("json").load("src/main/resources/1.json").show()
    //TODO 读取 CSV文件
     spark.read.format("csv").option("inferSchema", true.toString).option("header", "true")
      .load("src/main/resources/data1.csv").show()
    //val frame = spark.sql("select * from json.`src/main/resources/1.json`")
    //TODO 写入 mode类型
    //frame.write.format("json").mode("overwrite").save("src/main/resources/2.json")
    Thread.sleep(Int.MaxValue)
  }
}
