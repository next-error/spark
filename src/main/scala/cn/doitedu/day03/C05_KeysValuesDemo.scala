package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  如果rdd中的数量不是对偶元组，就不能使用keys、values方法
 */
object C05_KeysValuesDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr = Array(
      ("辽宁省", "沈阳市", 2000) , ("辽宁省", "大连市", 3000),
      ("山东省", "济南市", 2000) ,  ("山东省", "烟台市", 4000)
    )

    //RDD中对应的数据类型不再是对偶元组了，就不能使用keys和values方法了
    val rdd1: RDD[(String, String, Int)] = sc.parallelize(arr, 4)

    //但是可以使用map方法
    val res1 = rdd1.map(_._1)

    println(res1.collect().toBuffer)

    //5.释放资源
    sc.stop()


  }

}
