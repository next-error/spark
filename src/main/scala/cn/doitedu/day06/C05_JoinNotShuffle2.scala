package cn.doitedu.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * join不一定会shuffle

 *
 */
object C05_JoinNotShuffle2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val arr1 = Array(
      ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
      ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
      ("spark", 3), ("kafka", 3), ("hive", 3), ("hive", 3)
    )
    //通过并行化创建RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr1, 3)

    val arr2 = Array(
      ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
      ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4)
    )
    //通过并行化创建RDD
    val rdd2: RDD[(String, Int)] = sc.parallelize(arr2, 2)

    //分别对rdd1 和 rdd2 进行分区
    //groupByKey已经按照hashPartitioner分区了
    val rdd11 = rdd1.groupByKey()

    val rdd22 = rdd2.groupByKey()

    //该join不会shuffle，join默认也是使用的hashPartitioner，并且没有改变分区的数量
    val joined = rdd11.join(rdd22)

    sc.stop()

  }
}
