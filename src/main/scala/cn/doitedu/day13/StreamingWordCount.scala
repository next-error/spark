package cn.doitedu.day13

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用SparkStreaming编写一个WordCount程序
 *
 * 数据源：Socket客户端 nc -lk 8888
 * 如果没有nc命令，在linux执行 yum install nc
 *
 */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    //原来做离线计算需要先创建SparkContext
    //现在创建增量的SparkContext，即StreamingContext（可以定期生成小job）
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    //StreamingContext是对SparkContext的包装增强，并且要指定每个批次生成的时间间隔
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建抽象数据集，离线计算创建的是RDD，现在流式计算，创建DStream，增强的RDD，会定期生成RDD
    //在linux上事先启动nc 命令
    //nc -lk 8888
    val lines: DStream[String] = ssc.socketTextStream("node-1.51doit.cn", 8888)

    //实现WordCount
    //调用Dstream的方法，DStream的方法，也分为Transformation和Action
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //输出结果
    reduced.print() //Action

    //启动流式计算
    ssc.start() //定期生成job

    //不能退出，而是要一直执行（挂起）
    ssc.awaitTermination()



  }

}
