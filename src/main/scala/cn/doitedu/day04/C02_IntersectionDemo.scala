package cn.doitedu.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 求两个RDD的交集，例如统计一个系统的用户的次日留存
 * 前一天注册的用户有(a, b, c, d, e)
 * 第二个登录的用户  (a, c, f)
 */
object C02_IntersectionDemo {

  def main(args: Array[String]): Unit = {


    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "e"), 2)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "c", "f"), 2)

    //计算出两个RDD中共有的数据（交集）
    val rdd3: RDD[String] = rdd1.intersection(rdd2)
    val res = rdd3.collect()

    //不使用intersection，而是使用cogroup方法，实现类似intersection
    val rdd11: RDD[(String, Null)] = rdd1.map((_, null))
    val rdd22: RDD[(String, Null)] = rdd2.map((_, null))
    //将两个RDD进行cogroup
    val rdd33: RDD[(String, (Iterable[Null], Iterable[Null]))] = rdd11.cogroup(rdd22)
    val filtered = rdd33.filter { case (_, (it1, it2)) => {
      it1.nonEmpty && it2.nonEmpty
    }}
    val res2 = filtered.keys.collect()
    println(res2.toBuffer)



    println(res.toBuffer)

    //5.释放资源
    sc.stop()
  }

}
