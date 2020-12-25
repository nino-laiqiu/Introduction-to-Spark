package cn.sparksql.Day5


import com.alibaba.fastjson.JSONException
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test1 {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    var event: DataFrame = null
    //properties      map<string,string>,
    //releaseChannel  string,
    //resolution      string,
    //sessionId       string,
    //`timeStamp`       bigint
    val schema = new StructType(Array(
      StructField("account", DataTypes.StringType),
      StructField("appId", DataTypes.StringType),
      StructField("appVersion", DataTypes.StringType),
      StructField("carrier", DataTypes.StringType),
      StructField("appVersion", DataTypes.StringType),
      StructField("carrier", DataTypes.StringType),
      StructField("deviceId", DataTypes.StringType),
      StructField("deviceType", DataTypes.StringType),
      StructField("ip", DataTypes.StringType),
      StructField("latitude", DataTypes.StringType),
      StructField("longitude", DataTypes.StringType),
      StructField("netType", DataTypes.StringType),
      StructField("osName", DataTypes.StringType),
      StructField("osVersion", DataTypes.StringType),
      StructField("properties ",DataTypes.StringType ),
      StructField("releaseChannel", DataTypes.StringType),
    ))
    try {
      event = spark.read.json("src/main/resources/event.log")
    }
    catch {
      case x: Exception => logger.error(x + "json解析错误")
    }
    event.filter(
      'properties =!= null and
        'eventid =!= null and
        'sessionid =!= null,
    )
    spark.stop()

  }
}
