package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * aggregateByKey，是Transformation，一般会产生shuffle，但是特殊情况也可以不shuffle
 *
 * 比reduceByKey更灵活
 *
 */

object C09_AggregateByKeyDemo {


  def main(args: Array[String]): Unit = {


    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)


    val arr = Array(
      ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
      ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
      ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
      ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4),
    )


    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)

    //如果aggregateByKey指定了初始值，每个key会在出现的分区内，才会使用一次初始值
    val rdd2 = rdd1.aggregateByKey(0)(_+_, _+_)

    val res = rdd2.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }
}
