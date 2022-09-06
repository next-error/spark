package cn.doitedu.day03

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

/**
 * shuffledRDD是reduceByKey，groupByKey、combineKey、aggregateKey、foldByKey方法底层的具体实现
 *
 */

object C12_ShuffledRDDDemo {


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
    //使用并行化的方式，将客户端的集合转成RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)

    //不使用reduceByKey，而是使用new ShuffledRDD实现类似reduceByKey的功能
    val partitioner = new HashPartitioner(4)
    //如果只是new了一个ShuffledRDD传入一个分区器，相对数据仅完成了分区的功能
    val shuffledRDD: ShuffledRDD[String, Int, Int] = new ShuffledRDD[String, Int, Int](rdd1, partitioner)
    //指定聚合功能（要指定三个函数）
    val f1 = (x: Int) => x
    val f2 = (x: Int, y: Int) => x + y
    //val f3 = (x: Int, y: Int) => x + y
    val aggregator = new Aggregator[String, Int, Int](f1, f2, f2)
    //关联聚合器
    shuffledRDD.setAggregator(aggregator)
    //如果需要在map端进行聚合操作，设置mapSideCombine为true
    shuffledRDD.setMapSideCombine(true)
    //返回结果
    val res = shuffledRDD.collect()

    println(res.toBuffer)




    //5.释放资源
    sc.stop()


  }
}
