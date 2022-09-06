package cn.doitedu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 支持多种类型的join，如果调研join方法，就是inner join
 * 必须是两个rdd中key形同的数据才能join上
 *
 */
object C04_JoinDemo3 {

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

    //使用cogroup实现join的功能
    //调用cogroup将两个RDD都进行协同分组

    val grouped: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    val rdd4 = grouped.flatMapValues{case (it1, it2) => {
      for(i <- it1.iterator; j <- it2.iterator) yield (i, j)
    }}

    val res = rdd4.collect()

    println(res.toBuffer)


    //5.释放资源
    sc.stop()





  }

}
