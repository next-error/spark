package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey，是Transformation，一般会产生shuffle，但是特殊情况也可以不shuffle
 *
 * 跟reduceByKey功能一样，只不过可以指定一个初始值而已
 *
 */

object C10_FoldByKeyDemo {


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

    //rdd1.reduceByKey(_+_)
    //初始值也会在该key出现的分区内使用一次
    val rdd2 = rdd1.foldByKey(0)(_+_)

    val res = rdd2.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }
}
