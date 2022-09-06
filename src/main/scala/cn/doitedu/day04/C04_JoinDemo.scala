package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 支持多种类型的join，如果调研join方法，就是inner join
 * 必须是两个rdd中key形同的数据才能join上
 *
 */
object C04_JoinDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //通过并行化的方式创建一个RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    //通过并行化的方式再创建一个RDD
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))

    //调用cogroup将两个RDD都进行协同分组
    //val grouped: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    //调用join，要求两个RDD中的数据都必须是(K, _)，K必须是形同类型
    //Array((tom,(1,1)), (tom,(2,1)), (jerry,(3,2)))
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val res = rdd3.collect()
    println(res.toBuffer)


    //5.释放资源
    sc.stop()





  }

}
