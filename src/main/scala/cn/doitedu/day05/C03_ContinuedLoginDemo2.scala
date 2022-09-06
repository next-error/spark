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
 * 实时方式2,先分区，将数据排序,如果排序？
 *
 *
 *
 */
object C03_ContinuedLoginDemo2 {

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

    //先分区，并且在分区内排序
    //repartitionAndSortWithinPartitions，重新分区，并且在分区内排序
    //implicit val ord: Ordering[(String, String)] = Ordering[String].on[(String, String)](t => t._2).reverse
    implicit val ord: Ordering[(String, String)] =new Ordering[(String, String)] {
      override def compare(x: (String, String), y: (String, String)): Int = {
        - x._2.compareTo(y._2)
      }
    }
    val res = uidDtAndNull.repartitionAndSortWithinPartitions(uidPartitioner)

    res.saveAsTextFile("/Users/start/Desktop/out8")

    sc.stop()


  }

}

