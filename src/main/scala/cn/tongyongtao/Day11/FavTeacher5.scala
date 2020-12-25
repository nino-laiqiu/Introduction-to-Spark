package cn.tongyongtao.Day11

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object FavTeacher5 {

  def main(args: Array[String]): Unit = {

    val isLocal = true
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    if(isLocal) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD
    val lines: RDD[String] = sc.textFile("src/main/resources/topN.txt")
    //整理数据
    val topN = 3

    //
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(l => {
      val fields = l.split("/")
      val url = fields(2)
      val subject = url.substring(0, url.indexOf("."))
      val teacher = fields(3)
      ((subject, teacher), 1)
    })

    val subjects = subjectTeacherAndOne.map(_._1._1).distinct().collect()

    val subjectPartitioner = new SubjectPartitioner2(subjects)

    //调用reduceByKey时传入自定义的分区器
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(subjectPartitioner, _+_)

    //在分区内进行排序
    val res: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //定义一个可排序的集合TreeSet
      implicit val ord: Ordering[((String, String), Int)] = Ordering[Int].on[((String, String), Int)](t => t._2).reverse
      val sorter = new mutable.TreeSet[((String, String), Int)]()
      //变量迭代器
      it.foreach(t => {
        //将当前的这一条数据放入到treeSet中
        sorter.add(t)
        if (sorter.size > topN) {
          //移除最小的
          val last = sorter.last
          sorter -= last
        }
      })
      sorter.iterator
    })

    println(res.collect().toBuffer)

    sc.stop()

  }
}

class SubjectPartitioner2(val subjects: Array[String]) extends Partitioner {

  private val rules = new mutable.HashMap[String, Int]()
  var index = 0
  //初始化一个分区的规则
  for (sb <- subjects) {
    rules(sb) = index
    index += 1
  }

  //有多少学科就有多少分区
  override def numPartitions: Int = subjects.length

  //getPartition方法是在Executor中的Task在ShuffleWrite之前执行的
  override def getPartition(key: Any): Int = {
    //(String, String)
    val subject = key.asInstanceOf[(String, String)]._1
    //从map中取出对应学科的分区编好
    rules(subject)
  }
}
