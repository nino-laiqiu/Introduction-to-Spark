package cn.sparksql.Day3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val schema = new StructType(Array(
      StructField("id", DataTypes.IntegerType),
      StructField("start_time", DataTypes.StringType),
      StructField("last_time", DataTypes.StringType),
      StructField("flow", DataTypes.DoubleType)
    ))
    val frame = spark.read.format("csv").schema(schema).option("header", true.toString).load("src/main/resources/data1.csv")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    frame.select(
      'id,
      'start_time,
      'last_time,
      'flow,
      expr("lag(last_time,1,start_time) over(partition by id order by last_time) as lag_time")
    ).select(
      'id,
      'start_time,
      'last_time,
      'flow,
      expr("if(to_unix_timestamp(start_time,'yyyy/M/dd HH:mm') - to_unix_timestamp(lag_time,'yyyy/M/dd HH:mm') > 600, 1, 0) as flag")
    ).select(
      'id,
      'start_time,
      'last_time,
      'flow,
      sum("flag") over (Window.partitionBy("id").orderBy("start_time").rangeBetween(Window.unboundedPreceding, Window.currentRow)) as "sum_flag"
    ).groupBy(
      'id,
      'sum_flag
    ).agg(
      min('start_time) as "start_time",
      max('last_time) as "last_time",
      sum('flow)
    ).drop('sum_flag)
      .orderBy('id,'start_time).show()

  }
}
