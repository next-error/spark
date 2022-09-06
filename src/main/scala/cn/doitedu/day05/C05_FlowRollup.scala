package cn.doitedu.day05

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FlowRollup {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("data/data.csv")

    //使用mapPartitions对数据进行处理，一个分区中的多条数据使用同一个SimpleDateFormat
    val res = lines.mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(e => {
        val fields = e.split(",")
        val uid = fields(0)
        val startTime = dateFormat.parse(fields(1)).getTime
        val endTime = dateFormat.parse(fields(2)).getTime
        val downFlow = fields(3).toLong
        (uid, (startTime, endTime, downFlow))
      })
    }).groupByKey().flatMapValues(it => {
      //(起始时间戳，结束时间戳，下行流量)
      val sorted: List[(Long, Long, Long)] = it.toList.sortBy(_._1)
      var temp = 0L
      var flag = 0 //0 或 1
      var sum = 0 //0,0, 0一组 1，1，1 另一种  2 2 2 又是一组
      sorted.map(e => {
        val startTime = e._1
        val endTime = e._2
        val flow = e._3
        if(temp != 0) {
          if((startTime - temp) / (1000 * 60) > 10) {
            flag = 1
          } else {
            flag = 0
          }
        }
        temp = endTime
        sum += flag
        (startTime, endTime, flow, sum)
      })
    }).map{
      case (uid, (startTime, endTime, flow, sum)) => {
        ((uid, sum), (flow, startTime, endTime))
      }
    }.reduceByKey((a, b) => {
      (a._1 + b._1, Math.min(a._2, b._2), Math.max(a._3, b._3))
    }).mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map{
        case ((uid, _), (flow, startTime, endTime)) => {
          (uid, dateFormat.format(new Date(startTime)), dateFormat.format(new Date(endTime)), flow)
        }
      }
    }).collect()

    println(res.toBuffer)

    sc.stop()


  }

}
