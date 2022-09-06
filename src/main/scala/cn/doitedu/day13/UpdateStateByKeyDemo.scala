package cn.doitedu.day13

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 将每个批次key相同的数据进行累加，并且与历史的key相同的数据进行累加
 * 即将当前批次的数据进行累加，再累加历史数据
 */
object UpdateStateByKeyDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") //设置日志级别
    val ssc = new StreamingContext(sc, Seconds(5))

    //如果在SparkStreaming程序中，使用了状态，必须设置checkpoint目录，保存状态数据
    //设置checkpoint目录，这段文件系统的目录，hdfs或本地系统
    ssc.checkpoint("./ck")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node-1.51doit.cn", 8888)

    val words = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //聚合当前批次，并与历史数据进行聚合
    //state：状态，中间结果数据或历史累加数据，状态数据也会保存对应的key和value
    //updateStateByKey将当前批次的key形态的数据进行累加，并且与历史数据key相同的进行累加
    //updateStateByKey输入的函数是：updateFunc: (Seq[V], Option[S]) => Option[S]
    //第一个参数：每个分区形同key聚合的value
    //第二个参数：初始值或中间累加的结果数据
    val updateFunc = (seq: Seq[Int], op: Option[Int]) => {
      Some(seq.sum + op.getOrElse(0))
    }

    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    //输出结果
    reduced.print()

    //启动
    ssc.start()

    //挂起
    ssc.awaitTermination()

  }


}
