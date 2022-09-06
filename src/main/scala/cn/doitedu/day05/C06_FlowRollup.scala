package cn.doitedu.day05

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.beans.BeanProperty
import scala.collection.mutable

object FlowCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/data.txt")

    //lines.count()

    //整理数据
    val tupleRdd: RDD[((String, Long, Long, Long), Null)] = lines.mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(line => {
        val fields = line.split(",")
        val uid = fields(0)
        val startTime = fields(1)
        val endTime = fields(2)
        val downFlow = fields(3).toLong
        val startTimestamp = dateFormat.parse(startTime).getTime
        val endTimestamp = dateFormat.parse(endTime).getTime
        ((uid, startTimestamp, endTimestamp, downFlow), null)
      })
    })

    //计算出用户的ID，返回一个数组
    val uids = tupleRdd.map(_._1._1).distinct().collect()
    implicit val sorter: Ordering[(String, Long, Long, Long)] = Ordering[Long].on[(String, Long, Long, Long)](t => t._2)
    //重新分区且排序
    val repartitionedAndSorted = tupleRdd.repartitionAndSortWithinPartitions(new UidPartitioner2(uids))

    val uidAndFlag: RDD[((String, Int), (Long, Long, Long))] = repartitionedAndSorted.mapPartitions(it => {
      var temp = 0L
      var flag = 0
      it.map(t => {
        val startTimestamp = t._1._2
        val endTimestamp = t._1._3
        if(temp != 0) {
          if((startTimestamp - temp) / 1000 / 60 > 10) {
            flag += 1
          } else {
            flag += 0
          }
        }
        temp = endTimestamp
        ((t._1._1, flag), (startTimestamp, endTimestamp, t._1._4))
      })
    })

    val res = uidAndFlag.reduceByKey((t1, t2) => {
      (Math.min(t1._1, t2._1), Math.max(t1._2, t2._2), t1._3 + t2._3)
    }).mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(t => {
        (t._1._1, dateFormat.format(new Date(t._2._1)), dateFormat.format(new Date(t._2._2)), t._2._3)
      })
    }).collect()

    println(res.toBuffer)


    sc.stop()

  }
}

class UidPartitioner2(val uids: Array[String]) extends Partitioner {

  val uidToNum = new mutable.HashMap[String, Int]()
  var i = 0
  for(uid <- uids) {
    uidToNum(uid) = i
    i += 1
  }

  override def numPartitions: Int = uids.length

  override def getPartition(key: Any): Int = {
    val uid = key.asInstanceOf[(String, Long, Long, Long)]._1
    uidToNum(uid)
  }
}