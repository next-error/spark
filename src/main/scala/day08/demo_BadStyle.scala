package day08

import org.apache.spark.{SparkConf, SparkContext}

object demo_BadStyle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(master = "local[*]").setAppName("login")
    val sc = new SparkContext(conf)
    val  rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    val rdd2 = sc.parallelize(Array((1,"tom"),(2,"jerry"),(3,"kitty")))
    //rdd嵌套
    //在一个transformation中调用其他RDD的transformation或Action,是不合法的
    //1.调用RDD的transformation和action必须在Driver调用
    //2.传入transformation和action的函数,在executor中执行,即RDD的调用必须要有SparkContext
    val rdd3 = rdd1.map(x => {
      rdd2.values.count() * x//这里rdd2在executor中调用,没有SparkContext会报错
    })


    sc.stop()

  }

}
