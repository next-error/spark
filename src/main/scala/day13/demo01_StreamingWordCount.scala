package day13

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object demo01_StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    //原来离线版需要创建SparkContext,现在创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //创建抽象数据集,离线使用的是RDD,流式计算创建DStream:增强的EDD,可以定期生成
    //需要事先在linux上启动 nc -lk 8888 命令
    val lines: DStream[String] = ssc.socketTextStream("linux01", 8888)

    //实现WordCount功能
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val reduced = wordAndOne.reduceByKey(_ + _)

    //输出结果

    reduced.print()//启动Action

    //启动流式计算
    ssc.start() //定期生成job

    //不能退出,要挂起
    ssc.awaitTermination()



  }

}
