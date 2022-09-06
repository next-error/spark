package cn.doitedu.day06

import java.text.SimpleDateFormat

import org.apache.commons.lang.time.FastDateFormat

object DateUtils extends Serializable {

  //线程不安全
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //线程安全的
  //val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  //2022-05-23 11:39:30
  def parse(str: String): Long = {
    val date = sdf.parse(str)
    date.getTime
  }

  //synchronized 加锁，在一个Executor中有多个Task，每个Task就是一个线程
  //多个线程使用了加锁的方法，会导致在同一个时间点，只能有一个线程调用该方法
  def parse2(str: String): Long = synchronized {
    val date = sdf.parse(str)
    date.getTime
  }

}
