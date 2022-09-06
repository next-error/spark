package cn.doitedu.day08

import java.util

import cn.doitedu.day03.OrderBean
import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 写Spark程序的反面教材
 *
 */
object C03_BadStyle {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    val rdd2 = sc.parallelize(Array((1, "tom"), (2, "jerry"), (3, "kitty")))

    //RDD调用Transformation或Action，传入的函数，与其他的RDD有嵌套
    //异常信息
    //RDD transformations and actions are NOT invoked by the driver, but inside of other transformations;
    // (调用RDD的转换算子或Action，并不是咋Driver调用的，而是在其他的Transformation中调用了)
    // 例如，在一个Transformation中，调用其他RDD的Transformation或Action，是不合法的
    // for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063

    //1.SparkContext和RDD必须是在Driver创建
    //2.调用RDD的Transformation 和 Action必须在Driver调用
    //3.传入到Transformation和Action中的函数，是在Executor中执行的
    //val r = rdd2.values.count()

    val rdd3 = rdd1.map(x => {
      //函数是在Executor中执行，但是RDD调用Transformation和Action必须在Driver端（必须得有SparkContext）
      //Executor中没有SparkContext， This RDD lacks a SparkContext.
      rdd2.values.count() * x
    })

//    val rdd3: RDD[RDD[(String, Int)]] = rdd1.map(n => {
//      rdd2.map(t => {
//        if (t._1 == n) {
//          (t._2, n)
//        } else {
//          (t._2, t._1)
//        }
//      })
//    })

    val res = rdd3.collect()

    println(res.toBuffer)


    sc.stop()


  }

}
