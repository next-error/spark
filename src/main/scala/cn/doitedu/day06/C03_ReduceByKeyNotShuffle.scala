package cn.doitedu.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * ReduceByKey不一定会shuffle
 *
 * 如果一个RDD，先调用了一个shuffle的算子，完成了一次shuffle（使用了指定的分区器，默认是HashPartitioner），
 * shuffle后得到一个新的RDD，如果再次调用shuffle算子，并且使用了跟上次shuffle时相同类型的分区器，
 * 并且没有改变分区数量，后面的shuffle算子可以不shuffle
 *
 */
object C03_ReduceByKeyNotShuffle {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val arr = Array(
      ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
      ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
      ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
      ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4),
    )
    //通过并行化创建RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)
    //调用partitionBy传入HashPartitioner进行分区（分区数量为4）
    val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(rdd1.partitions.length))
    //调用reduceByKey进行聚合
    //val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _, 3)
    //将结果collect到Driver端
    val res = rdd3.collect()
    println(res.toBuffer)
    sc.stop()
  }
}
