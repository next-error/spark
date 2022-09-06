package day03

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OrderToHbase {
  def main(args: Array[String]): Unit = {
    //1.创建sparkcontext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //调用sc的source方法,创建RDD
    val lines = sc.textFile("data/exercise/order.json")
    val filtered: RDD[OrderBean] = lines.map(line => {
      var orderBean: OrderBean = null
      try {
        orderBean = JSON.parseObject(line, classOf[OrderBean])

      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
      orderBean
    }).filter(_ != null)

    //使用高德地图接口查询位置信息,发送HTTp请求
    //逆地理位置解析:输入经纬度返回城市



    sc.stop()
  }

}
case class OrderBean(oid:String,cid:Int, money:Double, longitude:Double, latitude:Double)