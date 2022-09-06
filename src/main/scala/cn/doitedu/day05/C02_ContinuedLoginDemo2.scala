package cn.doitedu.day05

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 使用RDD统计连续登录的用户\
 *
 * 实现方式1，不使用先分组，然后再组内排序，因为在组内排序（toList.sorted），如果一个组呢的数据过多，会带着内存溢出
 *
 * 实时方式2,先分区，将数据排序,如果排序(不是将数据全部放入内存中排序，是内存+磁盘）
 *
 * 一个用户对应一个分区，如果有1亿个用户，是不是就需要1亿个分区，以后会生成过多的Task，太浪费资源
 *
 * 第二种方式适用于：分区并不多，但是每个分区中的数据量比较多
 *
 *
 *
 */
object C02_ContinuedLoginDemo2 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile("data/test1.txt")

    val uidAndDt = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).distinct()

    //为了构建UidPartitioner，必须先触发一个Action，将全部的用户的数量统计出来（不能使用采样）
    //将全部的uid返回到Driver
    val uids: Array[String] = uidAndDt.map(_._1).distinct().collect()
    val uidPartitioner = new UidPartitioner(uids)

    //为了将参与排序的字段也放到key中，所有对数据进行整理
    //把参与排序的字段dt，跟uid联合前来作为key
    val uidDtAndNull: RDD[((String, String), Null)] = uidAndDt.map(t => ((t._1, t._2), null))

    //先分区，然后再分区内排序
    //自定义一个分区器，同一个用户进入到同一个分区内
    //对uidAndDt，对应的数据是(uid, dt)
    //partitionBy底层是new ShuffledRDD，传入指定的分区器，仅仅实现了分区
    //val partitioned: RDD[(String, String)] = uidAndDt.partitionBy(uidPartitioner)
    //mapPartitions将一个分区中的数据取出来
    //    partitioned.mapPartitions(it => {
    //      还是将数据toList放入到内存中排序
    //      val sorted = it.toList.sortBy(_._2)
    //      var i = 0
    //      //...
    //      it
    //    })

    //先分区，并且在分区内排序
    val shuffled: ShuffledRDD[(String, String), Null, Null] = new ShuffledRDD[(String, String), Null, Null](uidDtAndNull, uidPartitioner)
    //再进行排序,参与排序的字段必须在key里面
    //scala中，元组也有排序规则，implicit def Tuple2[T1, T2](implicit ord1: Ordering[T1], ord2: Ordering[T2]): Ordering[(T1, T2)] =
    //    new Tuple2Ordering(ord1, ord2)
    //设置排序规则setKeyOrdering，在每个分区内进行排序
    shuffled.setKeyOrdering(Ordering[(String, String)])

    //shuffled.saveAsTextFile("/Users/start/Desktop/out5")

    shuffled.mapPartitions(it => {


      it
    })


    sc.stop()


  }

}

class UidPartitioner(val uids: Array[String]) extends Partitioner {
  //规则分区规则
  val uidToIndex = new mutable.HashMap[String, Int]()
  var index = 0
  for (uid <- uids) {
    uidToIndex(uid) = index
    index += 1
  }

  //shuffle后新的RDD分区的数量
  override def numPartitions: Int = uids.length

  //在上游根据key计算对应的分区编号
  override def getPartition(key: Any): Int = {
    val uid = key.asInstanceOf[(String, String)]._1
    //根据uid计算对应uid的分区
    val index = uidToIndex(uid)
    index
  }
}