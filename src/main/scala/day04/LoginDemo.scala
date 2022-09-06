package day04

import org.apache.spark._
import org.apache.spark.rdd.RDD


/**
 * 分区排序最优方案:
 *
 */

object LoginDemo {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile("data/order.txt")

    val uidAndDt = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).distinct()

    //分区,(分区使用HashPartitioner)并且要排序,(要排序的字段必须在key中)
    val uidDtAndNull: RDD[((String, String), Null)] = uidAndDt.map((_, null))
    val partitioner = new HashPartitioner(uidDtAndNull.partitions.length)
    //repartitionAndSortWithinPartitions需要传入一个隐士参数,Order[(String,String)]
    val partitioned: RDD[((String, String), Null)] = uidDtAndNull.repartitionAndSortWithinPartitions(partitioner)
    uidAndDt.foreach(println)
    //partitioned.saveAsTextFile("d://g//cc/out1")

    sc.stop()

  }
}
  class MyHashPartitioner(val partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val uid = key.asInstanceOf[(String,String)]._1
  uid match {
    case null => 0
    case _ =>nonNegativeMod(uid.hashCode,numPartitions)
  }

    }
    private def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }

}
