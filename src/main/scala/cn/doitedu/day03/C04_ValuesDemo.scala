package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示values算子的使用，values是Transformation，只能针对于RDD中对应的是对偶元组类型的数据使用
 */
object C04_ValuesDemo {

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

    //将RDD中的对偶元组中的第二个元素返回，不会去重
    val values: RDD[Int] = rdd1.values

    //不使用keys方法，使用map方法，也可以实现
    val values2 = rdd1.map(_._2)


    val res = values.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
