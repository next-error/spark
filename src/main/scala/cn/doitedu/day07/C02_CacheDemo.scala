package cn.doitedu.day07

import java.net.InetAddress

import cn.doitedu.day06.RuleMapObjNotSer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/***
 *
 * 演示Spark的RDD中一个特殊的方法：cache
 * 1.cache方法即不是Transformation，也不是Action
 * 2.cache方法是Lazy的，当第一次触发Action是，才会读取数据，并且对应cache的RDD数据保存到内存中
 * 3.一个RDD以及其子RDD多次触发Action，cache才有意义
 * 4.cache是将数据缓存Executor进度的内存中，底层调用的是persist方法，默认的存储级别是StorageLevel.MEMORY_ONLY
 *
 */
object C02_CacheDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://node-1.51doit.cn:9000/ips")
    //将数据进程处理和过滤后再进行cache
    val filtered = rdd1.filter(_.startsWith("111"))
    //对RDD进行cache
    filtered.cache()

    //触发Action(第一次)
    val r1 = filtered.count()

    //对其子RDD触发Action（第二次）
    val upper = filtered.map(_.toUpperCase())
    upper.saveAsTextFile("hdfs://node-1.51doit.cn:9000/out56790")

    sc.stop()

  }
}

