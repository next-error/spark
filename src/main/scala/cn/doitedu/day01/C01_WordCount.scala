package cn.doitedu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 感受Spark编写分布式WordCount
 */
object C01_WordCount {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext（在Driver端创建）
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD（神奇的大集合，分布式的迭代器，以后可以生成很多Task）
    //调用Spark的数据源方法创建原始的RDD
    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和1组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    //(spark,1), (spark,1),    (hive,1), (hive,1)
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //reduced.foreach(println)
    reduced.saveAsTextFile(args(1))

    sc.stop()
  }

}
