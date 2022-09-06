package cn.doitedu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD的map方法的使用
 *
 * map是一个转换算子，对RDD中对应的数据进行映射操作
 *
 * RDD调用map方法，不会对数据立即进行计算，而是记录对哪个RDD，调用了map方法，传入什么函数
 *
 */
object C03_MapDemo2 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //2.调用SparkContext的Source方法，创建RDD(spark上下文，用于创建原始RDD）
    val lines: RDD[String] = sc.textFile("data/user.txt")

    //3.调用RDD的转换算子（Transformation（s）），调用完转换算子后，会生成新的RDD
    val tpRdd: RDD[(String, Int, Double)] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt + 10
      val fv = fields(2).toDouble
      (name, age, fv)
    })

    //4.调用行动算子（Action，生成Tasks，然后将Task提交到集群中执行或在本地执行）
    tpRdd.saveAsTextFile("out2")

    //5.释放资源
    sc.stop()



  }

}
