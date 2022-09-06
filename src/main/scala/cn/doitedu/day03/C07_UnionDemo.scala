package cn.doitedu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  union是Transformation算子，并且不会shuffle，将两个RDD合并到一起，生成一个新的RDD
 *
 *  要union的RDD，对应的数据类型必须一致
 *
 *  得到新的RDD后，新的RDD分区数量是原来union之前RDD的分区数量之和
 *
 *
 */
object C07_UnionDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建2个RDD
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5), 2)
    val rdd2: RDD[Int] = sc.parallelize(Array(4,5,6,7,8,9), 3)
    val rdd4: RDD[Int] = sc.parallelize(Array(4,5,8,9), 2)


    val rdd3: RDD[Int] = rdd1.union(rdd2)
    //val rdd3: RDD[Int] = rdd1 ++ rdd2
    //如果超过两个RDD进行union，必须两个rdd union后，再跟下一个进行union

    //将两个RDD合并一起后，进行统一的处理
    val rdd5 = rdd3.map(_ * 10)

    val res = rdd3.collect()



    println(res.toBuffer)

    //5.释放资源
    sc.stop()


  }

}
