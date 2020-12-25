package cn.sparksql.Day1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Test1 {
  def main(args: Array[String]): Unit = {
    //Todo 环境
    val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    //todo DataFrame执行
    //1.DataFrame => sql
    val dsql = session.read.json("src/main/resources/1.json")
    //dsql.show()
    // dsql.createOrReplaceTempView("user")
    //session.sql("select * from user").show()
    //2.DataFrame => DSL
    // dsql.select("name").show()
    //dsql.select($"name").show()
    // dsql.select('age + 1,'age).show()
    //TODO DataSet执行
    //val seq = Seq(1, 2, 3, 4, 5)
    //val ds = seq.toDS()
    //ds.show()
    //TODO RDD => DataFrame
    val rdd = session.sparkContext.makeRDD(List(("小明", 12, 2000), ("小华", 14, 9000)))
    val frame: DataFrame = rdd.toDF("name", "age", "money")
    //val rdd1: RDD[Row] = frame.rdd
    //frame.show()
    //rdd1.foreach(println)
    //TODO DataFrame => DataSet
    //val value: Dataset[User] = frame.as[User]
    //value.show()
    //val frame1: DataFrame = value.toDF()
    //TODO  RDD => DataSet
    val value1: Dataset[User] = rdd.map(data => data match {
      case (x, y, z) => User(x, y, z)
    }).toDS()
    // value1.show()
    // val rdd2: RDD[User] = value.rdd
    session.stop()
  }
}

case class User(name: String, age: Int, money: Double)
