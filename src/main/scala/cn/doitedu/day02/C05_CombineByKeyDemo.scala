package cn.doitedu.day02

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

/** *
 * 演示spark的算子CombineByKey，spark中对key进行操作比较灵活的算子，是Transformation
 */
object C05_CombineByKeyDemo {


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

    val rdd2: RDD[(String, Int)] = rdd1.combineByKey(f1, f2, f3);

    val res = rdd2.collect
    //
    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }
}
