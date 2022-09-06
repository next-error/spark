package cn.doitedu.day03

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示keys算子的使用，keys是Transformation，只能针对于RDD中对应的是对偶元组类型的数据使用
 */
object C03_KeysDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    val arr = Array(
      ("spark", 1), ("hive", 1), ("hadoop", 1), ("spark", 1),
      ("spark", 2), ("kafka", 2), ("hive", 2), ("hive", 2),
      ("spark", 3), ("spark", 3), ("hadoop", 3), ("hadoop", 3),
      ("spark", 4), ("spark", 4), ("kafka", 4), ("kafka", 4),
    )
    //使用并行化的方式，将客户端的集合转成RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(arr, 4)

    //将RDD中的对偶元组中的第一个元素返回，不会去重
    //有隐式转换，有一个隐式转换方法或函数，将RDD[(K, V)] => PairRDDFunctions[K, V]
    //该隐式转换在object RDD
    /**
     * object RDD {
     *   implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
     *    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
     *    new PairRDDFunctions(rdd)
     *   }
     * }
      */
    //val keys: RDD[String] = rdd1.keys

    //不使用隐式转换，也要调用keys方法
    //而是自己显式的调用一个包装类进行包装，然后再调用包装类中扩展的方法keys
    val pairRDDFunc: PairRDDFunctions[String, Int] = new PairRDDFunctions[String, Int](rdd1)
    val keys = pairRDDFunc.keys

    //不使用keys方法，使用map方法，也可以实现
    //val keys2 = rdd1.map(_._1)




    val res = keys.collect()

    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
