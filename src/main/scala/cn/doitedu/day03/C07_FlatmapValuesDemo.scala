package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatmapValues算子的使用，该方法也是定义在PairRDDFunction中的
 * 将value的数据打平后再跟key组合在对偶元组中
 */
object C07_FlatmapValuesDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    val arr = Array(
      ("spark", "1 2 3"), ("hive", "4 5 6"), ("flink", "7 8 9")
    )
    //(spark,1), (spark, 2), (spark, 3), (hive, 4), (hive, 5)....
    //使用并行化的方式，将客户端的集合转成RDD
    val rdd1: RDD[(String, String)] = sc.parallelize(arr, 4)

    val rdd2: RDD[(String, Int)] = rdd1.flatMapValues(str => {
      val arr = str.split(" ")
      arr.map(_.toInt)
    })
    val res = rdd2.collect()
    println(res.toBuffer)


    //不使用flatMapValues，而且要实现与flatMapValues方法一样的功能
    //提示：使用flatMap
    val rdd3: RDD[(String, Int)] = rdd1.flatMap { case (k, v) => {
      val nums = v.split(" ")
      val arr: Array[(String, Int)] = nums.map(s => (k, s.toInt))
      arr
    }}

    val res2: Array[(String, Int)] = rdd3.collect()

    println(res2.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
