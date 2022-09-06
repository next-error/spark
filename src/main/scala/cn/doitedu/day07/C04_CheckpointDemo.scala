package cn.doitedu.day07

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/***
 *
 * 演示Spark的RDD中一个特殊的方法：checkpoint，可以RDD的中间结果保存到HDFS中
 * 1.checkpoint方法即不是Transformation，也不是Action
 * 2.checkpoint方法是Lazy的，当第一次触发Action是，会额外触发一个job，是将数据处理后保存到HDFS中
 * 3.一个RDD以及其子RDD多次触发Action，checkpoint才有意义
 * 4.一个RDD调用了checkpoint方法，这个RDD一起的依赖关系就被移除了
 * 5.如果一个RDD即调用了cache，又调用了checkpoint，多次触发Action，会优先使用cache的数据，如果cache的数据丢失，再使用checkpoint的数据
 */
object C04_CheckpointDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //设置checkpoint的存储目录
    sc.setCheckpointDir("hdfs://node-1.51doit.cn:9000/chk01")


    val rdd1 = sc.textFile("hdfs://node-1.51doit.cn:9000/ips")
    //将数据进程处理和过滤后再进行checkpoint
    val filtered = rdd1.filter(_.startsWith("111"))
    //调用checkpoint
    filtered.checkpoint()


    //触发Action(第一次)
    val r1 = filtered.count()

    //对其子RDD触发Action（第二次）
    val upper = filtered.map(_.toUpperCase())
    upper.saveAsTextFile("hdfs://node-1.51doit.cn:9000/out56790")



    sc.stop()

  }
}

