package cn.doitedu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

/** *
 * GroupByKey底层三个函数是否执行了
 * groupByKey 由于设置mapSideCombine = false, 只执行了两个函数，并且这个两个函数都在下游执行的
 *
 */
object C08_CombineByKeyDemo {


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
      println("f111 invoked ~~~ in stage: " + TaskContext.get().stageId())
      ArrayBuffer(x)
    }
    //在分区内（上游），将再次出现key相同的value，进行处理
    val f2 = (ab: ArrayBuffer[Int], b: Int) => {
      println("f222 invoked @@@ in stage: " + TaskContext.get().stageId())
      ab += b
    }
    //将相同key每个分区聚合聚合的结果，在进行全局聚合（下游）
    val f3 = (ab1: ArrayBuffer[Int], ab2: ArrayBuffer[Int]) => {
      println("f333 invoked $$$ in stage: " + TaskContext.get().stageId())
      ab1 ++= ab2
    }

    //使用GroupByKey，底层mapSideCombine = false，只有前两个函数执行了，第三个函数没有执行，并且前两个函数是在下游执行的
    val rdd2: RDD[(String, ArrayBuffer[Int])] = rdd1.combineByKeyWithClassTag(f1, f2, f3, new HashPartitioner(4), false);

    val res = rdd2.collect
    //
    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }
}
