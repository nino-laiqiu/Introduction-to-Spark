package cn.sparksql.Day3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Test6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sprak = SparkSession.builder().config(conf).getOrCreate()
    import sprak.implicits._
    import org.apache.spark.sql.functions._
    val schema = new StructType(Array(
      StructField("id", DataTypes.StringType),
      StructField("start_time", DataTypes.StringType),
      StructField("money", DataTypes.DoubleType),
    ))
    val frame = sprak.read.format("csv").schema(schema).option("header", false.toString).load("src/main/resources/shop.csv")
    frame.select(
      'id,
      substring('start_time, 0, 7) as "time",
      'money
    ).groupBy('id, 'time)
      .agg(sum($"money")as("sum_money"))
     .select(
        'id,
        'time,
        sum($"sum_money").over(Window.partitionBy("id").orderBy("time").rangeBetween(Window.unboundedPreceding,Window.currentRow))as("sum_")
      ).orderBy("id","time").show()
  }
}
