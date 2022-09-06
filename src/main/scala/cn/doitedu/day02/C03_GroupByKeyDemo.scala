package cn.doitedu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * GroupByKey按照指定的key进行分组，key相同的会进入到同一个组内
 */
object C03_GroupByKeyDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)


   val arr = Array(
     ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
     ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
     ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
     ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4),
   )

    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)

    //按照key进行分组（ 即按照单词进行分组）
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()

    //将结果写入到文件系统
    rdd2.saveAsTextFile("file:///Users/start/Desktop/out")


    //5.释放资源
    sc.stop()


  }

}
