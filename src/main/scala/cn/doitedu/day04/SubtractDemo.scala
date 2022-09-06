package cn.doitedu.day04

import scala.collection.mutable._

object SubtractDemo {

  def main(args: Array[String]): Unit = {

    //RDD的subtract方法底层调用的是subtractByKey，再底层new SubtractRDD


    //SubtractRDD内部有一个compute方法，先shuffle，将两个RDD， key相同的是搞到同一台机器的同一个分区内

    //将来自第一个RDD的数据放入到HashMap中
    //Array("a", "a", "b", "c", "c", "d")
    val mp = Map("a"-> ArrayBuffer(null, null), "b" -> ArrayBuffer(null), "c" -> ArrayBuffer(null, null), "d" -> ArrayBuffer(null))

    //将来自第二个RDD中的数据Array("a", "b")
    mp -= "a"
    mp -= "b"

    val res = mp.flatMap{case (k, v) => {
      v.map((k, _))
    }}.map(_._1)

    println(res)



  }
}
