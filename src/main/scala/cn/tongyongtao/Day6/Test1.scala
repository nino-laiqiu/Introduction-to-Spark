package cn.tongyongtao.Day6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//1. 读取文件的数据test.txt
//2. 一共有多少个小于20岁的人参加考试？
//3. 一共有多少个等于20岁的人参加考试？
//4. 一共有多少个大于20岁的人参加考试？
//5. 一共有多个男生参加考试？
//6. 一共有多少个女生参加考试？
//7. 12班有多少人参加考试？
//8. 13班有多少人参加考试？
//9. 语文科目的平均成绩是多少？
//10. 数学科目的平均成绩是多少？
object Test1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test1").setMaster("local[*]"))
    val rdd = sc.textFile("src/main/resources/sanguo.txt")
    rdd.map(data => {
      val number = data.split(" ")
      ((number(4), number(5).toInt))
    }).filter(_._1 == "chinese").aggregateByKey((0, 0))(
      (u, v) => ((u._1 + v), u._2 + 1),
      (x, x1) => ((x._1 + x1._1), (x._2 + x1._2))
    ).mapValues(data => data._1 / data._2).collect.foreach(println)
    sc.stop()
  }
}
