package cn.doitedu.day06

import java.text.SimpleDateFormat

class DateUtilsClassNotSer {

  //线程不安全
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //2022-05-23 11:39:30
  def parse(str: String): Long = {
    val date = sdf.parse(str)
    date.getTime
  }

}
