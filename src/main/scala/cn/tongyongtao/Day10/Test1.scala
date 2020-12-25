package cn.tongyongtao.Day10

import java.lang.Exception
import java.sql.{Connection, Date, DriverManager, PreparedStatement, Statement}

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
//形如
//{"oid":"o129", "cid": 3, "money": 300.0, "longitude":115.398128,"latitude":35.916527}
//{"oid":"o130", "cid": 2, "money": 100.0, "longitude":116.397128,"latitude":39.916527}
//{"oid":"o131", "cid": 1, "money": 100.0, "longitude":117.394128,"latitude":38.916527}
//{"oid":"o132", "cid": 3, "money": 200.0, "longitude":118.396128,"latitude":35.916527}
//1,家具
//2,手机
//3,服装

//try-with-resources语法
object Test1 {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("order").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/order.txt")
    //读取要join的数据
    val rdd1 = sc.textFile("src/main/resources/busness.txt")

    //读取业务数据
    val operationrdd: RDD[(Int, Double)] = rdd.map(data => {
      //使用temp来接收,如果不用怎么获取try中的数据
      var temp: (Int, Double) = null
      try {
        val json = JSON.parseObject(data)
        val cid = json.getInteger("cid").toInt
        val money = json.getDouble("money").toDouble
        temp = (cid, money)
      }
      catch {
        case e: JSONException => logger.error("json解析错误" + e)
      }
      temp
    })
    //过滤脏数据
    val filter = operationrdd.filter(_ != null)
    //把money相加
    val conbinerdd: RDD[(Int, Double)] = filter.combineByKey(
      v => v,
      (u: Double, v: Double) => u + v,
      (x1: Double, x2: Double) => x1 + x2
    )
    //读取join的数据
    val joinrdd: RDD[(Int, String)] = rdd1.map(data => {
      val strings = data.split(",")
      (strings(0).toInt, strings(1).toString)
    })
    val value: RDD[(Int, (Double, Option[String]))] = conbinerdd.leftOuterJoin(joinrdd)
    var conn: Connection = null
    var statement: PreparedStatement = null
    value.foreachPartition(data => {

      try {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_demo4?characterEncoding=utf8", "root", "123456")
        //创建Statement
        statement = conn.prepareStatement("insert  into  tb_myorder_spark values(?,?,?,?)")
        data.foreach(data1 => {
          statement.setDate(1, new Date(System.currentTimeMillis()))
          statement.setInt(2, data1._1)
          statement.setDouble(3, data1._2._1)
          statement.setString(4, data1._2._2.getOrElse("未知").toString)
          statement.executeUpdate()
        })
      }
      catch {
        case e: Exception => logger.error("jdbc" + e)
      }
      finally {
        if (statement != null && conn != null) {
          statement.close()
          conn.close()
        }
      }
    })
    sc.stop()
  }
}

//使用jdbc创建连接
/*    var conn: Connection = null
var statement: PreparedStatement = null
//在diver端创建
try {
conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_demo4?characterEncoding=utf8", "root", "123456")
//创建Statement
//发现问题:SQL语句insert不会写了.....
statement = conn.prepareStatement("insert  into  tb_myorder_spark values(?,?,?,?)")
value.collect.foreach(data => {
 statement.setDate(1, new Date(System.currentTimeMillis()))
 statement.setInt(2, data._1)
 statement.setDouble(3, data._2._1)
 statement.setString(4, data._2._2.getOrElse("未知").toString)
 statement.executeUpdate()
})
} catch {
case e: Exception => logger.error("jdbc" + e)
}
finally {
if (statement != null && conn != null) {
 statement.close()
 conn.close()
}
}*/

