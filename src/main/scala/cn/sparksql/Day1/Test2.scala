package cn.sparksql.Day1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//简单的UDF函数
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val framejson = spark.read.json("src/main/resources/1.json")
    framejson.createOrReplaceTempView("user")
    spark.udf.register("udfname",(name:String) => {
         "Name:" + name
    })
    spark.sql("select udfname(name) , age from user").show()
    spark.stop()
  }
}
