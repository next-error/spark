package cn.doitedu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/** *
 * 演示spark的算子CombineByKey，spark中对key进行操作比较灵活的算子，是Transformation
 */
object C06_CombineByKeyDemo2 {


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

    //使用combineByKey实现GroupByKey的功能

    val f1 = (x: Int) => ListBuffer(x) //在分区内，将第一次出现的key的value放入到一个ListBuffer中
    val f2 = (buff: ListBuffer[Int], a: Int) => buff += a //将key相同再次的value追加到原来的ListBuffer中
    val f3 = (buff1: ListBuffer[Int], buff2: ListBuffer[Int]) => buff1 ++= buff2 //将没法分区key相同的value合并到一起


    val rdd2: RDD[(String, ListBuffer[Int])] = rdd1.combineByKey(f1, f2, f3);

    rdd2.saveAsTextFile("file:///Users/start/Desktop/out2")

    //5.释放资源
    sc.stop()


  }
}
