package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * distinct，是Transformation，一般会产生shuffle，但是特殊情况也可以不shuffle
 *
 * distinct是将RDD中对应的数据进行去重（分布式去重）
 *
 */

object C11_DistinctDemo {


  def main(args: Array[String]): Unit = {


    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)


    val arr = Array(
      "spark", "hive", "spark", "flink",
      "spark", "hive", "hive", "flink",
      "flink", "flink", "flink", "spark"
    )

    val rdd1: RDD[String] = sc.parallelize(arr, 3)
    //去重
    val rdd2 = rdd1.distinct()
    val res = rdd2.collect()
    println(res.toBuffer)

    //不直接使用distinct方法，实现类似distinct功能

    //去重是相同的数据shuffle到同一台机器的同一个分区内，同一个组，
    //数据需要shuffle，spark中如果要shuffle，就要把数据变成RDD[(K, V)]

    val rdd11: RDD[(String, Null)] = rdd1.map((_, null))
    val rdd12: RDD[String] = rdd11.reduceByKey((a, _) => a).keys
    val res2 = rdd12.collect()
    println(res2.toBuffer)


    //5.释放资源
    sc.stop()


  }
}
