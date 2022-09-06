package cn.doitedu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * 使用RDD统计连续登录的用户\
 *
 * 分区且排序的最优解决方案：
 *   1.使用类似HashPartitioner进行分区，可以保证同一个用户的数据进入到同一个分区内，但是一个分区内可能有多个用户的数据
 *   2.在分区的同时，并且之前排序规则，按照用户id和日期联合起来排序，同一个用户ID的数据按照日期排序，并且是连续的
 *
 * 解决了前两个程序的痛点
 *
 */
object C04_ContinuedLoginDemo3 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile("data/test1.txt")

    //将数据整理，然后按照用户ID和日期进行去重
    val uidAndDt = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).distinct()

    //分区（数据必须整理成(k,v), 分区使用HashPartitioner），并且排序（要排序的字段必须在key中）
    val uidDtAndNull: RDD[((String, String), Null)] = uidAndDt.map((_, null))

    val partitioner = new MyHashPartitioner(uidDtAndNull.partitions.length)
    //repartitionAndSortWithinPartitions需要传入一个隐式参数，是Order[(String, String)],
    val partitioned: RDD[((String, String), Null)] = uidDtAndNull.repartitionAndSortWithinPartitions(partitioner)

    val res = partitioned.mapPartitions(it => {
      var rn = 0
      it.map { case ((uid, dt), _) => {
        rn += 1
        (uid, dt, rn)
      }}
    })

    val r = res.collect

    println(r.toBuffer)

    sc.stop()


  }

}

class MyHashPartitioner(val partitions: Int) extends Partitioner {


  override def numPartitions: Int = partitions

  //在shuffle的时候，传入的key其实是(uid, dt)
  override def getPartition(key: Any): Int = {
    val uid = key.asInstanceOf[(String, String)]._1
    uid match {
      case null => 0
      case _ => nonNegativeMod(uid.hashCode, numPartitions)
    }
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}