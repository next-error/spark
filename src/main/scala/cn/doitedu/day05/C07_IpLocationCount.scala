package cn.doitedu.day05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取IP规则数据，然后将IP规则数据进行广播，
 * 再读取用户的行为日志，关联缓存的广播数据
 */
object C07_IpLocationCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //读取ip规则数据
    val ipLines: RDD[String] = sc.textFile("data/ip.txt")

    //将数据收集到Driver端
    val ipRulesInDriver: Array[(Long, Long, String, String)] = ipLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      (startNum, endNum, province, city)
    }).collect()

    //将Driver端的ip规则广播出去
    //broadcast方法是阻塞的，该方法没有执行完，会阻塞，不会进行执行
    //而且广播后，会将广播到每个Executor的数据的信息返回Driver，broadcastRef保存了数据存储在Executor的内存地址
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRulesInDriver)

    //读取行为的行为日志
    val logLines: RDD[String] = sc.textFile("data/ipaccess.txt")

    val provinceAndOne = logLines.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1)
      //将ip地址转成十进制
      val ipNum = IpUtils.ip2Long(ip)
      //获取事先已经广播到Executor中广播变量（ip规则）
      val ipRulesInExecutor = broadcastRef.value
      //二分法查找
      val index = IpUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if (index >= 0) {
        province = ipRulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced = provinceAndOne.reduceByKey(_ + _)

    val res = reduced.collect()

    println(res.toBuffer)

    sc.stop()
  }

}
