package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo_GroupByKey {
  /**
   * 按照指定的key分组,相同的key分到同一个组内,只适合对偶元组类型(a,b)
   * GroupBy(),需要传入按照哪个key分组,是个多个数据的类型(a,b,c)
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("mapDemo")
    val sc = new SparkContext(conf)


    val arr = Array(("spark",1),("hive",1),("hadoop",1),("spark",1),
      ("spark",2),("kafka",2),("jive",2),("hive",2),
      ("spark",3),("spark",3),("hadoop",3),("hive",3),
      ("spark",4),("spark",4),("kafka",4),("kafka",4))

    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    rdd2.saveAsTextFile("d://g//cc//d")

   sc.stop()
  }
}
