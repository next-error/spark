package day13
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从Kafka中读取数据，并且将数据进行聚合，然后将聚合的结果写入到MySQL（支持事务）
 *
 * 实现ExactlyOnce，数据不漏读，也不会多读，实现精准一次性语义
 *
 * 1.处理一个批次，开启一次事务（MySQL的事务）
 * 2.将聚合的结果，返回到Driver，写入到MySQL中
 * 3.将这个批次对应的偏移量，也写入到MySQL中
 * 4.提交事务（如果出现异常，回滚事务）
 *
 * 可以保证，计算好的几个，和这个批次对应的偏移量，要成功都写入到MySQL中，如果失败都放弃
 * 程序重启后，先读取Mysql的历史偏移量，接着读
 *
 */
object ExactlyOnceWordCount {

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

    //查询历史偏移量
    val historyOffsets: Map[TopicPartition, Long] = OffsetUtils.queryHistoryOffsetFromMySQL(appName, groupId)

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

        var conn: Connection = null
        var pstm: PreparedStatement = null
        try {
          conn = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456")
          //开启事务
          conn.setAutoCommit(false)
          pstm = conn.prepareStatement("INSERT INTO tb_wordcount values (?, ?) ON DUPLICATE KEY UPDATE counts = counts + ?")
          //将计算好的结果，和偏移量，在同一事务中，写入到MySQL
          for (tp <- res) {
            pstm.setString(1, tp._1)
            pstm.setInt(2, tp._2)
            pstm.setInt(3, tp._2)
            pstm.executeUpdate()
          }
          //appName_groupId, topic_partition, offset
          pstm = conn.prepareStatement("INSERT INTO tb_history_offset values (?, ?, ?) ON DUPLICATE KEY UPDATE `offset` = ?")
          //将偏移量写入到数据中
          for (or <- offsetRanges) {
            val topic = or.topic
            val partition = or.partition
            val offset = or.untilOffset
            pstm.setString(1, appName + "_" + groupId)
            pstm.setString(2, topic + "_" + partition)
            pstm.setLong(3, offset)
            pstm.setLong(4, offset)
            pstm.executeUpdate()
          }
          //提交事务
          conn.commit()
        } catch {
          case e: Exception => {
            conn.rollback() //回滚事务
            //停止程序
            ssc.stop(true)
          }
        } finally {
          if(pstm != null) pstm.close()
          if(conn != null) conn.close()
        }
      }
    })



    //启动流式计算
    ssc.start() //定期生成job

    //不能退出，而是要一直执行（挂起）
    ssc.awaitTermination()


  }

}
