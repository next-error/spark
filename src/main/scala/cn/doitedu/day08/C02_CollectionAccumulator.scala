package cn.doitedu.day08

import java.util

import cn.doitedu.day03.OrderBean
import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 *  collection类型的累加器，是一种特殊类型的累加器（数据收集器），
 *  可以将每个分区的特立类型的数据返回到Driver端
 *
 */
object C02_CollectionAccumulator {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("data/order.json")

    //val collectionAcc = new ArrayBuffer[String]()
    val collectionAcc: CollectionAccumulator[String] = sc.collectionAccumulator[String]("error-acc")

    val beanRDD: RDD[OrderBean] = rdd1.map(line => {

      var bean: OrderBean = null
      try {
        bean = JSON.parseObject(line, classOf[OrderBean])
      } catch {
        case e: JSONException => {
          //将有问题的数据收集起来，将特殊的数据返回到Driver端
          collectionAcc.add(line)
        }
      }
      bean
    })

    val filtered: RDD[OrderBean] = beanRDD.filter(_ != null)


    val res = filtered.count()

    val errorData: util.List[String] = collectionAcc.value

    //如果在Scala中，使用for循环，变量Java集合
    //导入隐式转换
    import scala.collection.JavaConverters._
    //使用增强的方法，将java集合，转成Scala集合
    for(e <- errorData.asScala) {
      println(e)
    }


    sc.stop()


  }

}
