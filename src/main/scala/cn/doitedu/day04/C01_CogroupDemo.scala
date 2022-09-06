package cn.doitedu.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * cogroup 代表协同分组、联合分组
 *
 * groupByKey、groupBy的区别是
 *    groupByKey、groupBy只能对一个RDD进行分组
 *    cogroup可以对多个RDD进行分组，将多个RDD中key相同的数据通过shuffle，分到同一个分区的同一个组内
 *
 *
 * cogroup要求多个RDD中对应的数据类型必须是RDD[(K, _)], 只要多个RDD中的K类型相同就可以cogroup
 *
 */
object C01_CogroupDemo {

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

    //将两个RDD都进行分组
    val grouped: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    // Array(
    // (tom,(CompactBuffer(1, 2),CompactBuffer(1))), //tom在第一个RDD中出现2次，在第二个RDD中出现1次
    // (jerry,(CompactBuffer(3),CompactBuffer(2))),  //jerry第一个RDD中出现1次，在第二个RDD中出现1次
    // (shuke,(CompactBuffer(),CompactBuffer(2))),   //shuke第一个RDD中出现0次，在第二个RDD中出现1次
    // (kitty,(CompactBuffer(2),CompactBuffer()))    //kitty第一个RDD中出现1次，在第二个RDD中出现0次
    // )
    val res = grouped.collect()

    println(res.toBuffer)


    //5.释放资源
    sc.stop()





  }

}
