package cn.doitedu.day07

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/***
 *
 * 演示Spark的RDD中一个特殊的方法：persist
 * 1.persist方法即不是Transformation，也不是Action
 * 2.persist方法是Lazy的，当第一次触发Action是，才会读取数据，并且对应persist的RDD数据保存到内存中或Executor对应的磁盘
 * 3.一个RDD以及其子RDD多次触发Action，persist才有意义
 * 4.persist方法可以指定多种存储级别
 * 5.以后在开发中，如果需要提高程序的执行效率，可以建议调用persist，存储级别为内存+磁盘，并行序列化
 * 6.shuffle会将中间结果，写入到本地磁盘，多次触发Action，后面的Stage和复用，所以说，Shuffle保存的中间结果，是一种特殊的persist
 */
object C03_PersistDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //spark建议使用Kryo作为其默认的序列化方式
    //Kryo速度更快（序列化和反序列化），占用空间小（shuffle的数据要在网络间传输和persist要缓存数据到内存或本地磁盘）
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://node-1.51doit.cn:9000/ips")
    //将数据进程处理和过滤后再进行cache
    val filtered = rdd1.filter(_.startsWith("111"))
    //对RDD进行persist
    //存储级别StorageLevel.MEMORY_AND_DISK_SER，将数据使用指定的方式进行序列化，优先缓存到内存中，内存放不下，再将放不下的数据保存到本地磁盘
    filtered.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //触发Action(第一次)
    val r1 = filtered.count()

    //对其子RDD触发Action（第二次）
    val upper = filtered.map(_.toUpperCase())
    upper.saveAsTextFile("hdfs://node-1.51doit.cn:9000/out56790")


    //是否缓存的数据调用unpersist方法
    filtered.unpersist(false) //如果传入的参数为true，即该方法为阻塞的

    sc.stop()

  }
}

