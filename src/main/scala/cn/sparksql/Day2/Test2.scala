package cn.sparksql.Day2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/db_demo4?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "tb_data1")
      .option("user", "root")
      .option("password", "123456")
      .load()
    frame.createOrReplaceTempView("tb_data1")
   // spark.sql("select TIMESTAMPDIFF(SECOND,'2018-07-04 12:01:00','2018-07-04 12:00:00');")
    //spark.sql("select UNIX_TIMESTAMP('2020/2/09 20:23')- UNIX_TIMESTAMP('2020/2/09 20:23')").show()
    spark.sql(
      """
        |select
        |id,min(starttime),max(lasttime),sum(flow)
        |from
        |(
        |select
        |id, starttime, lasttime, flow,
        |sum(number1) over( partition by id order by starttime rows between unbounded preceding and current row) as timeorder
        |from (
        |select
        |id, starttime, lasttime, flow, if(number<10,0,1) as number1
        |from
        |(
        |select
        |id, starttime, lasttime, flow, flagtime ,if(flagtime=0,0,(UNIX_TIMESTAMP(starttime,'yyyy/M/dd HH:mm')- UNIX_TIMESTAMP(flagtime,'yyyy/M/dd HH:mm'))/60) as number
        |from (
        |select
        |*,
        |lag(lasttime,1,0) over (partition by id order by lasttime) as flagtime
        |from tb_data1 ) t1 ) t2 ) t3  ) t4
        |group by  id,timeorder;
        |""".stripMargin).show()
    spark.sql(
      """
        |select
        |id, starttime, lasttime, flow,
        |sum(number1) over( partition by id order by starttime) as timeorder
        |from (
        |select
        |id, starttime, lasttime, flow, if(number<10,0,1) as number1
        |from
        |(
        |select
        |id, starttime, lasttime, flow, flagtime ,if(flagtime=0,0,(UNIX_TIMESTAMP(starttime,'yyyy/M/dd HH:mm')- UNIX_TIMESTAMP(flagtime,'yyyy/M/dd HH:mm'))/60) as number
        |from (
        |select
        |*,
        |lag(lasttime,1,0) over (partition by id order by lasttime) as flagtime
        |from tb_data1 ) t1 ) t2 ) t3
        |""".stripMargin)
         spark.sql(
           """
             |select
             |id, starttime, lasttime, flow, if(number<10,0,1) as number1
             |from
             |(
             |select
             |id, starttime, lasttime, flow, flagtime ,if(flagtime=0,0,(UNIX_TIMESTAMP(starttime,'yyyy/M/dd HH:mm')- UNIX_TIMESTAMP(flagtime,'yyyy/M/dd HH:mm'))/60) as number
             |from (
             |select
             |*,
             |lag(lasttime,1,0) over (partition by id order by lasttime) as flagtime
             |from tb_data1 ) t1 ) t2
             |""".stripMargin)
    Thread.sleep(Int.MaxValue)
  }
}
