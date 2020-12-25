package cn.sparksql.Day3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // spark.sql("select  date_format('2020-2-01 20:20' ,'yyyy-MM-dd HH:mm');").show()
    val schema = new StructType(Array(
      StructField("id", DataTypes.IntegerType),
      StructField("start_time", DataTypes.StringType),
      StructField("last_time", DataTypes.StringType),
      StructField("flow", DataTypes.DoubleType)
    ))

  }
}
