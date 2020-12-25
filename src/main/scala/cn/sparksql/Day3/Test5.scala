package cn.sparksql.Day3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Test5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sprak = SparkSession.builder().config(conf).getOrCreate()
    import sprak.implicits._
    import org.apache.spark.sql.functions._

    val schema = new StructType(Array(
      StructField("id", DataTypes.StringType),
      StructField("start_time", DataTypes.StringType),
    ))
    val frame = sprak.read.format("csv").schema(schema).option("header", false.toString).load("src/main/resources/date1.csv")
    frame.distinct().select(
      'id,
      'start_time,
      dense_rank().over(Window.partitionBy("id").orderBy("start_time")) as ("rn")
    ).select(
      'id,
      'start_time,
      date_sub('start_time, 'rn) as "last_time", //spark.2不支持
      // expr("date_sub(start_time,rn) as last_time")
    ).groupBy(
      'id,
      'last_time
    ).agg(
      min("start_time") as "min_time",
      max("start_time") as "max_time",
      count("id") as "times"
    ).where('times >= 3).show()
  }
}
