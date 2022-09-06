package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark操作的是一个更加强大的分布式集合 (RDD) , 分布式迭代器
 */
object Work_Count {
  def main(args: Array[String]): Unit = {
    //创建RDD(弹性的,分布式数据集,可以生成多个task)
    //创建SparkContext
    val conf = new SparkConf()
    //设置执行模式
    conf.setMaster("local[*]")
    conf.setAppName("wordcount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //wordAndOne.foreach(println)
    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //reduced.foreach(println)
    reduced.saveAsTextFile(args(1))


    sc.stop()


  }

}
