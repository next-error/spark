package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 支持多种类型的join，如果调研join方法，就是inner join
 * 必须是两个rdd中key形同的数据才能join上
 *
 */
object C04_JoinDemo2 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //通过并行化的方式创建一个RDD
    val arr1 = Array(
      ("spark", 0), ("spark", 10),
      ("kafka", 1), ("flink", 1),
    )
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr1, 2)

    val arr2 = Array(
      ("spark", 21), ("spark", 22),
      ("kafka", 3), ("flink", 3),
      ("kafka", 4), ("flink", 4),
    )
    val rdd2: RDD[(String, Int)] = sc.parallelize(arr2, 3)


    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    //Array(
    // (kafka,(1,3)), (kafka,(1,4)),
    // (flink,(1,4)), (flink,(1,3)),
    // (spark,(0,21)), (spark,(0,22)),
    // (spark,(10,21)), (spark,(10,22))
    // )
    val res = rdd3.collect()

    println(res.toBuffer)


    //5.释放资源
    sc.stop()





  }

}
