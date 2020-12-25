package cn.sparksql.Day7

import org.apache.spark.sql.SparkSession


//连续登陆案例分析
object ContinuouslandingSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.read.csv("src/main/resources/date1.csv").toDF("id","dt").createTempView("Continuouslanding")
    spark.sql(
      """
        |select
        |id,
        |min(dt),
        |max(dt),
        |count(landing)
        |from
        |(
        |select
        |id,dt,
        |date_sub(dt,rn) as landing
        |from
        |(
        |select
        |id,dt,
        |row_number() over(partition by id order by dt) as rn
        |from
        |(
        |select
        |distinct id,dt
        |from
        |Continuouslanding
        |) t1
        |) t2
        |) t3
        |group by id , landing
        |""".stripMargin).show(100,false)
  }
}
