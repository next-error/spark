package cn.doitedu.day05

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用RDD统计连续登录的用户
 */
object C01_ContinuedLoginDemo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]") //设置执行模式
    conf.setAppName("WordCount")
    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lines: RDD[String] = sc.textFile("data/test1.txt")

    val uidAndDt = lines.map(line => {
      val fields = line.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).distinct()

    //Driver创建的
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    //分组,并且在组内排序
    val grouped: RDD[(String, Iterable[String])] = uidAndDt.groupByKey()
    val uidDiffAndDt = grouped.flatMapValues(it => {
      //将数据放到内存中，是为了在内存中进行排序
      val sorted = it.toList.sorted
      //定义一个可变的变量i
      var i = 0
      //(线程安全的问题，序列化的问题)

      val calendar = Calendar.getInstance()
      sorted.map(dt => {
        i += 1
        val date = sdf.parse(dt)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -i)
        val diff = sdf.format(calendar.getTime)
        (dt, diff)
      })
    }).map { case (uid, (dt, diff)) => {
      ((uid, diff), (dt, dt, 1))
    }}
    //对Values进行处理
    val res = uidDiffAndDt.reduceByKey((a, b) => {
      (Ordering[String].min(a._1, b._1), Ordering[String].max(a._2, b._2), a._3 + b._3)
    }).map{case ((uid, _), (start, end, count)) => {
      (uid, start, end, count)
    }}.filter(_._4 >= 3)

    val r = res.collect

    println(r.toBuffer)

    sc.stop()


  }

}
