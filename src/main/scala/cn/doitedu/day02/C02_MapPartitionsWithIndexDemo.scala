package cn.doitedu.day02

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitionsWithIndex是Transformation，与map的功能类似，但是在一些特殊场景下，比map算子效率更高
 *
 * MapPartitionsWithIndex，取出分区对应的迭代器的同时，可以把分区编号取出来
 *
 *
 *
 */
object C02_MapPartitionsWithIndexDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //创建一个RDD
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val rdd1: RDD[Int] = sc.makeRDD(arr, 2)

    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map(e => {
        (index, e)
      })
    })

    val res = rdd2.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
