package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd的count方法是一个Action，会将rdd中对应的元素的数量返回到Driver
 */
object C12_CountDemo {

  //Spark提供了一个更加强大的分布式集合（RDD），分布式的迭代器
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //
    val lines: RDD[String] = sc.textFile("hdfs://node-1.51doit.cn:9000/words")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    //RDD有多个分区，会生成多个Task，每个task先进行局部计数，然后将没法分区计数的结果返回到Driver，在进行全局的计数
    val res: Long = words.count()

    //val res = sorted.collect()
    println(res)

    sc.stop()
  }

}
