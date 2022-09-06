package cn.doitedu.jedis.day02

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.doitedu.day13.OffsetUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Transaction

/**
 * 从Kafka中读取数据，并且将数据进行聚合，然后将聚合的结果写入到MySQL（支持事务）
 *
 * 实现ExactlyOnce，数据不漏读，也不会多读，实现精准一次性语义
 *
 * 1.处理一个批次，开启一次事务（Redis的事务）
 * 2.将聚合的结果，返回到Driver，写入到Redis中
 * 3.将这个批次对应的偏移量，也写入到Redis中
 * 4.提交事务（如果出现异常，回滚事务）
 *
 * 可以保证，计算好的结果，和这个批次对应的偏移量，要成功都写入到Redis中，如果失败都放弃
 * 程序重启后，先读取Redis的历史偏移量，接着读
 *
 */
object ExactlyOnceWordCountToRedis {

  def main(args: Array[String]): Unit = {

    val appName = "swc"
    val groupId = "g02"
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    //StreamingContext是对SparkContext的包装增强，并且要指定每个批次生成的时间间隔
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN") //设置日志级别
    //指定以后从Kafka中读取数据，创建DStream
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //不自动提交偏移量
    )

    val topics = Array("wordcount")

    //创建从Kafka中读取数据的DStream
    //DirectStream（直连数据流）指的是生成的每个Task（消费者），直接连到Kafka的Leader分区上

    //从Redis中查询历史偏移量
    val historyOffsets: Map[TopicPartition, Long] = OffsetUtils.queryHistoryOffsetFromRedis(appName, groupId)

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //位置策略，指定消费的最优策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, historyOffsets) //消费策略
    )

    //使用foreachRDD方法，将每个批次的RDD取出来，并且调用RDD的API对数据进行运算
    //foreachRDD方法既不是Transformation，也不是Action，只是将DStream中定期生成的RDD取出来
    //val linesDStream: DStream[String] = kafkaStream.map(_.value())
    kafkaStream.foreachRDD(rdd => {
      //如果当前批次对应的RDD中，不为空，再取出偏移量，再对数据进行处理，然后再提交偏移量
      if (!rdd.isEmpty()) {
        //只有第一手的KafkaRDD可以被转成HasOffsetRanges，可以获取偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //对RDD进行操作(调用RDD的API)
        val lines = rdd.map(_.value())
        val reduced = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        //将聚合好的结果，返回到Driver端
        val res: Array[(String, Int)] = reduced.collect

        //获取一个Jedis连接
        val jedis = JedisConnectionPool.getJedis()

        var transaction: Transaction = null
        try {
          //开启事务,返回一个带事务的连接
          transaction = jedis.multi()

          //将当前批次计算好的结果写入到Redis中，还要与历史数据进行累加（在Redis中进行累加）
          for (tp <- res) {
            //结果保存到hash类型的value中
            transaction.hincrBy("WORD_COUNT", tp._1, tp._2)
          }
          //将最新的偏移量也写入到Redis中
          for (or <- offsetRanges) {
            val topic = or.topic
            val partition = or.partition
            val untilOffset = or.untilOffset
            transaction.hset(appName + "_" + groupId, topic + "_" + partition, untilOffset.toString)
          }
          //提交事务
          transaction.exec()
        } catch {
          case e: Exception => {
            //废弃事务
            transaction.discard()
            //停止程序
            ssc.stop(true)
          }
        } finally {
          if(transaction != null) transaction.close()
          //没有关闭连接，程序写入几个批次后，就没法再写入数据了
          if(jedis != null) jedis.close()
        }
      }
    })



    //启动流式计算
    ssc.start() //定期生成job

    //不能退出，而是要一直执行（挂起）
    ssc.awaitTermination()


  }

}
