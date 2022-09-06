package cn.doitedu.day13

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从Kafka中读取数据，实现WordCount的功能
 * 1.导入跟Kafka整合的依赖
 * <dependency>
 *     <groupId>org.apache.spark</groupId>
 *     <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
 *     <version>3.2.1</version>
 * </dependency>
 *
 */
object KafkaStreamingWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    //StreamingContext是对SparkContext的包装增强，并且要指定每个批次生成的时间间隔
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN") //设置日志级别
    //指定以后从Kafka中读取数据，创建DStream
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "abc01",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //不自动提交偏移量
    )

    val topics = Array("wordcount")

    //创建从Kafka中读取数据的DStream
    //DirectStream（直连数据流）指的是生成的每个Task（消费者），直接连到Kafka的Leader分区上

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //位置策略，指定消费的最优策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消费策略
    )

    //最数据进行操作
    //实现WordCount
    //调用Dstream的方法，DStream的方法，也分为Transformation和Action
    val lines: DStream[String] = kafkaStream.map(cr => cr.value())

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
