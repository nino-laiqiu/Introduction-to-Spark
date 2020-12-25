package cn.sparksql.Day7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object GroupedIntoSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val schema = new StructType(Array(
      StructField("id", DataTypes.StringType),
      StructField("begin_time", DataTypes.StringType),
      StructField("end_time", DataTypes.StringType),
      StructField("down_flow", DataTypes.DoubleType),
    ))
    val frame = spark.read.format("csv").schema(schema).load("C:\\Users\\hp\\IdeaProjects\\Spark_Maven\\src\\main\\resources\\data.csv")
    frame.createTempView("GroupedInto")

    spark.sql(
      """
        |select
        |id,
        |min(begin_time) as begin_time,
        |max(end_time) as end_time,
        |sum(down_flow) as down_flow
        |from
        |(
        |select
        |id,begin_time,end_time,down_flow,
        |sum(flag) over(partition by id order by begin_time
        |rows between unbounded preceding and current row ) as sum_flag
        |from
        |(
        |select
        |id,begin_time,end_time,down_flow,
        |if((to_unix_timestamp(begin_time,'yyyy/M/dd HH:mm') -
        |to_unix_timestamp(rn_time,'yyyy/M/dd HH:mm') )> 600, 1, 0) as flag
        |from
        |(
        |select
        |id,begin_time,end_time,down_flow,
        |lag(end_time,1,begin_time) over(partition by id order by begin_time ) as rn_time
        |from
        |GroupedInto
        |) t1
        |) t2
        |) t3
        |group by id , sum_flag
        |""".stripMargin).show(100)
  }
}
