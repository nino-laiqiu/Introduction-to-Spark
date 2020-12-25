package cn.sparksql.Day8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object ContinuouslandingDsl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val dataFrame = spark.read.csv("src/main/resources/date1.csv").toDF("id", "dt")
    dataFrame
      .distinct().select(
      col("id")
      , col("dt")
      , row_number().over(Window.partitionBy("id").orderBy(col("dt"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)) as "rn"
    )
      .select(
        col("id")
        , col("dt")
        , date_sub(col("dt"), col("rn")) as "date_rn"
        //spark2.0不支持使用expr()
      )
      .groupBy("id", "date_rn")
      .agg(
        min(col("dt")) as "begin_time"
        , max(col("dt")) as "end_time"
        , count(col("date_rn")) as "number"
      )
      .select("id", "begin_time", "end_time", "number")
      // .drop(col("date_rn"))
      .show(100)
  }
}
