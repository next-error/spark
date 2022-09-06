package cn.doitedu.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

/**
 * Spark RDD的map方法的使用
 *
 * map是一个转换算子，对RDD中对应的数据进行映射操作
 *
 * RDD调用map方法，不会对数据立即进行计算，而是记录对哪个RDD，调用了map方法，传入什么函数
 *
 */
object C02_MapDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //2.调用SparkContext的Source方法，创建RDD(spark上下文，用于创建原始RDD）
    val lines: RDD[String] = sc.textFile("data/words.txt")

    //3.调用RDD的转换算子（Transformation（s）），调用完转换算子后，会生成新的RDD
    val upperLines: RDD[String] = lines.map(_.toUpperCase)

    //4.调用行动算子（Action，生成Tasks，然后将Task提交到集群中执行或在本地执行）
    upperLines.saveAsTextFile("out")

    //5.释放资源
    sc.stop()



  }

}
