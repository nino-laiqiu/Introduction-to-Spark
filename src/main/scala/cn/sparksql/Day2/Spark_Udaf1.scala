package cn.sparksql.Day2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object Spark_Udaf1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame = spark.read.json("src/main/resources/1.json")
    frame.createOrReplaceTempView("user")
    spark.udf.register("avgAge", new MyAvg)
    val sql: String = "select avgAge(age) from user"
    spark.sql(sql).show()
    Thread.sleep(Int.MaxValue)
    spark.stop()
  }
}

//统计年龄的总和,统计数据的条数
class MyAvg extends UserDefinedAggregateFunction {
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("age", LongType)))
  }

  //缓冲区的类型
  override def bufferSchema: StructType = {
    //第一个值:年龄的总和  第二个值:条数的总和
    StructType(Array(StructField("total", LongType), StructField("count", LongType)))
  }

  //输出的数据的类型
  override def dataType: DataType = LongType

  //函数是否稳定
  override def deterministic: Boolean = true

  //缓冲区初始数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)

    buffer.update(1, buffer.getLong(1) + 1)
    //上一个相当于下一个,底层调用了update方法
    println(buffer.getLong(1))
  }

  //缓冲区的合并,存在多个缓存区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //todo (x,y) =>第一个值是初始值或者中间值
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
    buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
  }

  //输出数据的类型
  override def evaluate(buffer: Row): Any = {
    println(buffer.getLong(0))
    println(buffer.getLong(1))
    buffer.getLong(0) / buffer.getLong(1)
  }
}
