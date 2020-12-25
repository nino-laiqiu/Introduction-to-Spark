package cn.sparksql.Day8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window



//分组转化
object GroupedIntoSqlDsl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val dataFrame = spark.read.csv("src/main/resources/data.csv").toDF("id", "begin_time", "end_time", "down_flow")
    import  spark.implicits._
    import org.apache.spark.sql.functions._
    dataFrame.selectExpr(
      "id"
      ,"begin_time"
      ,"end_time"
      ,"down_flow"
      ,"lag(end_time,1,begin_time) over(partition by id order by begin_time) as lag_time " )
      .select(
        'id
        ,'begin_time
        ,'end_time
        ,'down_flow
        //if (unix_timestamp('begin_time,"yyyy/M/dd HH:mm")-unix_timestamp(col("lag_time"),"yyyy/M/dd HH:mm") > 600 ,1,0)
        ,expr("if((to_unix_timestamp(begin_time,'yyyy/M/dd HH:mm')-to_unix_timestamp(lag_time ,'yyyy/M/dd HH:mm'))>600,1,0)") as "flag"
      )
      .select(
         col("id")
        ,col("begin_time")
        ,col("end_time")
        ,col("down_flow")
        ,sum(col("flag")).over( Window.partitionBy(col("id")).orderBy(col("begin_time")).rowsBetween(Window.unboundedPreceding,Window.currentRow)) as "rn"
      )
     .groupBy(col("id"),col("rn"))
     .agg(
       min(col("begin_time")) as "begin_time"
       ,max((col("end_time"))) as "end_time"
       ,sum(col("down_flow")) as "down_flow"
       )
       //要是后面有select 则不需要使用drop来过滤字段了
       .drop(col("rn"))
      .orderBy(col("id"))
      .select("id","begin_time","end_time","down_flow")
      .show(100,false)
  }
}
