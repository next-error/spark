package cn.doitedu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/** *
 * 使用combineByKeyWithClassTag实现特殊情况（聚合），mapSideCombine = false
 */
object C07_CombineByKeyDemo3 {


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

    //使用combineByKey实现ReduceByKey的功能
    //在分区内（上游），将该key第一次出现对应的value进行处理
    val f1 = (x: Int) => {
      println("f1 invoked ~~~ in stage: " + TaskContext.get().stageId())
      x
    }
    //在分区内（上游），将再次出现key相同的value，进行处理
    val f2 = (a: Int, b: Int) => {
      println("f2 invoked @@@ in stage: " + TaskContext.get().stageId())
      a + b
    }
    //将相同key每个分区聚合聚合的结果，在进行全局聚合（下游）
    val f3 = (m: Int, n: Int) => {
      println("f3 invoked $$$ in stage: " + TaskContext.get().stageId())
      m + n
    }

    //如果mapSideCombine = true 三个函数都会执行， 前两个函数在上游执行，第三个函数在下游执行
    //如果mapSideCombine = false, 三个函数只会执行前2个，并且这两个函数都是在下游执行的
    val rdd2: RDD[(String, Int)] = rdd1.combineByKeyWithClassTag(
      f1, f2, f3,
      new HashPartitioner(4), false
    )




    //5.释放资源
    sc.stop()


  }
}
