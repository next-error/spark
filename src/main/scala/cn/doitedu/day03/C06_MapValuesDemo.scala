package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  mapValues算子的使用，该方法也是定义在PairRDDFunction中的
 */
object C06_MapValuesDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr = Array(
      ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
      ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
      ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
      ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4),
    )
    //使用并行化的方式，将客户端的集合转成RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)

    //将每一个元素（对偶元组）中的第二个数据取出来，应用传入的函数
    //将返回的key再跟原来的key组合到一起
    val rdd11: RDD[(String, Int)] = rdd1.mapValues(_ * 10)

    val res1: Array[(String, Int)] = rdd11.collect()
    println(res1.toBuffer)

    //不使用mapValues方法，但是要实现相同的功能
    val rdd22 = rdd1.map(t => (t._1, t._2 * 10))
    val res2: Array[(String, Int)] = rdd22.collect()
    println(res2.toBuffer)


    //5.释放资源
    sc.stop()


  }

}
