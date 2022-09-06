package cn.doitedu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD的过滤方法，filter
 *
 * filter是一个转换算子，调用filter需要传入一个函数，函数的返回值是boolean类型，返回true会保留
 *
 * 调用RDD的filter方法不会立即触发任务执行，调用Action算子后，才会从前往后一次运行
 */
object C01_FilterDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD（神奇的大集合，分布式的迭代器，以后可以生成很多Task）
    //调用Spark的数据源方法创建原始的RDD
    val lines: RDD[String] = sc.textFile("data/words.txt")
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //过滤掉不是以h开头的单词
    val filtered: RDD[String] = words.filter(!_.startsWith("h"))

    //触发Action
    filtered.foreach(println)

    sc.stop()
  }

}
