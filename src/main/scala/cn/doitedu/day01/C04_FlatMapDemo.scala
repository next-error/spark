package cn.doitedu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD的 flatMap方法的使用
 *
 * flatMap是一个转换算子，对RDD中对应的数据进扁平化映射，相当于向map在flatten，但是RDD中没有flatten方法
 *
 * RDD调用flatMap方法，不会对数据立即进行计算，而是记录对哪个RDD，调用了flatMap方法，传入什么函数
 *
 * RDD是一个抽象的、弹性的，可容错的，分布式数据集，里面不装真正要计算的数据，而是装的描述信息
 * （描述以后从哪里读数据，对数据怎样进行计算）
 *
 */
object C04_FlatMapDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //2.调用SparkContext的Source方法，创建RDD(spark上下文，用于创建原始RDD）
    val lines: RDD[String] = sc.textFile("data/user.txt")

    //3.调用RDD的转换算子（Transformation（s）），调用完转换算子后，会生成新的RDD
    //val rdd2: RDD[Array[String]] = lines.map(_.split(" "))
    //rdd2.flatten // RDD没有flatten方法
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //4.调用行动算子（Action，生成Tasks，然后将Task提交到集群中执行或在本地执行）
    words.foreach(println)

    //5.释放资源
    sc.stop()



  }

}
