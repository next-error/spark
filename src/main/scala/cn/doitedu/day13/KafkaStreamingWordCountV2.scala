package cn.doitedu.day13

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从Kafka中读取数据，实现WordCount的功能，消费一个批次的数据，更新一次偏移量
 *
 */
object KafkaStreamingWordCountV2 {

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

    //使用foreachRDD方法，将每个批次的RDD取出来，并且调用RDD的API对数据进行运算
    //foreachRDD方法既不是Transformation，也不是Action，只是将DStream中定期生成的RDD取出来
    kafkaStream.foreachRDD(rdd => {
      //如果当前批次对应的RDD中，不为空，再取出偏移量，再对数据进行处理，然后再提交偏移量
      if(!rdd.isEmpty()) {
        //只有第一手的KafkaRDD可以被转成HasOffsetRanges，可以获取偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //对RDD进行操作(调用RDD的API)
        val lines = rdd.map(_.value())
        val reduced = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        val res = reduced.collect
        println(res.toBuffer)

        //提交偏移量，处理完一个批次，提交偏移量（偏移量写入到Kafka特殊的Topic中了__consumer_offsets）
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }

    })



    //启动流式计算
    ssc.start() //定期生成job

    //不能退出，而是要一直执行（挂起）
    ssc.awaitTermination()



  }

}
