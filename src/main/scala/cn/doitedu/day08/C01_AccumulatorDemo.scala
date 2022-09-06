package cn.doitedu.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark中的累计器，在job运行时，可以对数据进行统计，先在Executor进行统计每个分区的，然后再返回到Driver
 */
object C01_AccumulatorDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd1 = sc.parallelize(Array(1,2,3,4, 5,6,7,8,9), 2)

    //触发一次Action，统计出奇数的条数，并且将每个元素乘以10
//    val r1 = rdd1.filter(_ % 2 != 0).count() //触发一次
//    val rdd2 = rdd1.map(_ * 10)
//    val r2 = rdd2.collect() //触发一次


    //先不使用累加器，自己定义一个变量
    var acc = 0
    val rdd2 = rdd1.map(n => {
      if (n % 2 != 0) {
        acc += 1 //闭包
      }
      (n * 10, acc)
    })

    val res = rdd2.collect()
    //为什么acc的结果为 0 ？
    //并没有将Executor端task计算的数据返回给Driver
    println("acc的结果： " + acc)

    println(res.toBuffer)






    sc.stop()


  }

}
