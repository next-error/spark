package cn.doitedu.day02

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * MapPartitions是Transformation，与map的功能类似，但是在一些特殊场景下，比map算子效率更高
 *
 * 但是不说MapPartitions一定比map算子效率高
 *
 * 将RDD中对应的数据以分区的形式，一个分区就是一个迭代器
 *
 */
object C01_MapPartitionsDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //创建一个RDD
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val rdd1: RDD[Int] = sc.makeRDD(arr)

    //连接不能序列化，所以不能在Driver初始化
    //val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
    //将RDD中对应的数据以分区的形式取出来，一个分区对应一个迭代器
    val rdd3 = rdd1.map(e => {

      //创建数据库连接(map的函数中，每处理一条数据，就创建一个连接，使用一次就不用)
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
      //使用数据库连接
      e * 10

    })

    //访问外部的数据库，比较先建立连接
    //数据库连接不能定义在外边（Driver客户端），数据库的连接是不支持序列化的

    val rdd2: RDD[Int] = rdd1.mapPartitions(it => {
      //函数里面的逻辑，会在Executor中执行
      //先创建好一个连接（在Executor）中创建
      val conn = DriverManager.getConnection("")
      //处理一个分区的多条数据，可以使用同一个连接
      it.map(e => {

        //使用事先定义好的连接，查询数据库，关联一些维度数据
        //...
        e * 10
      })
    })

    val res = rdd2.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
