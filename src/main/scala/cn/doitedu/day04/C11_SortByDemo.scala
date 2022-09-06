package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将RDD中的数据按照指定的排序规则进行排序
 */
object C11_SortByDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD（神奇的大集合，分布式的迭代器，以后可以生成很多Task）
    //调用Spark的数据源方法创建原始的RDD
    val lines: RDD[String] = sc.textFile("hdfs://node-1.51doit.cn:9000/words")
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和1组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //按照单词出现的次数，从高到低进行排序
    //val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //val keyed: RDD[(Int, (String, Int))] = reduced.keyBy(_._2).sortByKey()
    val sorted = reduced.map(t => (t._2, t)).sortByKey(false)


    val res = sorted.collect()
    println(res.toBuffer)

    sc.stop()
  }

}
