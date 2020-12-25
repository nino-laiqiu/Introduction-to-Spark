package cn.sparksql.Day3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val structType = new StructType(Array(
      StructField("id", DataTypes.IntegerType),
      StructField("age", DataTypes.IntegerType)))
    val frame = spark.read.format("csv").option("header", true.toString).schema(structType).load("src/main/resources/11.csv")
    frame.createTempView("tb_my_data")
    spark.sql("select  distinct  id, age  from tb_my_data;").show()


  }
}
