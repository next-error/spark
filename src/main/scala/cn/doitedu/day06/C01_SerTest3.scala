package cn.doitedu.day06

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * 是对Spark的函数已经函数使用到了函数以为的引用对象（闭包）
 */
object C01_SerTest3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    //1,ln
    //2,sd
    //3,sh
    //4,hn
    val lines = sc.textFile(args(0))

    //初始化一个RuleMap对象(在Driver初始化的)
    //闭包：函数内部使用到了函数外部的引用类型
    //RuleMapObjSer实现了序列化
    val ruleMapObj = RuleMapObjSer

    val res = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0)
      val code = fields(1)
      //在函数中使用ruleMap对象
      val name = ruleMapObj.rules.getOrElse(code, "未知")
      //获取当前的线程ID
      val threadId = Thread.currentThread().getId
      //获取分别编号
      val partitionId = TaskContext.get().partitionId()
      //获取当前Task所在机器的主机名
      val hostname = InetAddress.getLocalHost.getHostName
      (id, code, name, threadId, partitionId, hostname, ruleMapObj)
    })

    res.saveAsTextFile(args(1))

    sc.stop()

  }


}
