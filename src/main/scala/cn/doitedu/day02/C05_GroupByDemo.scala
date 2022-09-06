package cn.doitedu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/** *
 * 演示groupBy的功能，groupBy与GroupByKey功能类似，但是groupBy更灵活
 * GroupByKey只能针对RDD中的数据为对偶元组，其他类型的数据不能调用GroupByKey
 *
 *
 */
object C05_GroupByDemo {


  def main(args: Array[String]): Unit = {


    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)


    val arr = Array(
      ("辽宁省", "沈阳市", 2000) , ("辽宁省", "大连市", 3000),
      ("山东省", "济南市", 2000) ,  ("山东省", "烟台市", 4000)
    )

    val rdd1: RDD[(String, String, Int)] = sc.parallelize(arr, 4)

    //将数据按照省份进行分组
    //gropuBy更灵活（value中重复保留的key的数据），但是groupByKey更高效一些
    val grouped: RDD[(String, Iterable[(String, String, Int)])] = rdd1.groupBy(_._1)

    //将数据整理后，使用groupByKey

    val rdd3: RDD[(String, (String, Int))] = rdd1.map { case (province, city, money) => {
      (province, (city, money))
    }}

    val rdd4 = rdd3.groupByKey()


    rdd1



    //5.释放资源
    sc.stop()


  }
}
