package day13

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/**
 * 从Kafka中读取数据,每消费一个批次的数据,更新一次偏移量
 */
object demo02_KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN") //设置日志级别
    //向Kafka中读取数据
    val kafkaParms: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "linux01:9092,linux02:9092,linux03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g001",
      "atuo.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("wordcount")
    //创建从Kafka中读取数据的DStream
    //DirecStream (直连数据流) 是指生成的每个Task(消费者) ,直接连到Kafka的Leader分区上
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //位置策略,指定消费的最优策略
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParms) //消费策略
    )

    //使用foreachRDD方法,将每个批次的RDD取出,并调用RDD方法进行运算
    //foreachRDD方法既不是Transaction也不是Action,只是定期将DStream中的RDD取出
    kafkaStream.foreachRDD(rdd => {
      //若果当前批次的RDD不为空,则取出偏移量进行运算,在提交偏移量
      if(!rdd.isEmpty()){
        //只有第一手的KafkaRDD可以被转化为HashOffsetRange,可以获取偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //对RDD进行WordCount操作
        val lines = rdd.map(_.value())
        val reduced = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        val res = reduced.collect
        println(res.toBuffer)

        //当前批次处理完成,提交偏移量 (偏移量写入了Kafka特殊的topic中)
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    })
      //启动流式计算
    ssc.start()

    ssc.awaitTermination()



  }
}
