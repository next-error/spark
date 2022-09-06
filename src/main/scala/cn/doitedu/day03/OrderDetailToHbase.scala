package cn.doitedu.day03

import java.util

import cn.doitedu.day03.HttpClientTest.key
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

/**
 * 使用spark将数据进行处理（转换、过滤，关联维度），然后将数据写入到Hbase中
 */
object OrderDetailToHbase {

  val key = "74e962e2f795114980cd33ce09d923d1"

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //只有以后从哪里读取数据
    val lines = sc.textFile("data/order.json")
    //解析json数据
    val filtered: RDD[OrderBean] = lines.map(line => {
      var orderBean: OrderBean = null
      try {
        orderBean = JSON.parseObject(line, classOf[OrderBean])
      } catch {
        case e: Exception => {
          //e.printStackTrace()
        }
      }
      orderBean
    }).filter(_ != null)

    //关联维度（使用高德地图接口查询位置信息，发送HTTP请求）
    //逆地理位置解析，即输入经纬度，返回省市区等信息
    val beanRDDWithLocation: RDD[OrderBean] = filtered.mapPartitions(it => {
      //创建一个HttpClient的连接
      val httpclient = HttpClients.createDefault
      //对迭代器进行map操作
      it.map(bean => {
        //发送查询请求
        val longitude = bean.longitude
        val latitude = bean.latitude
        val httpGet = new HttpGet(s"https://restapi.amap.com/v3/geocode/regeo?&location=$longitude,$latitude&key=$key")
        //发送http请求，进行查询
        val response = httpclient.execute(httpGet)
        //获取响应体
        val entity = response.getEntity
        var province: String = null
        var city: String = null
        //如果http请求的状态码为200,代表请求成功
        if (response.getStatusLine.getStatusCode == 200) {
          //获取请求的json字符串
          val result = EntityUtils.toString(entity)
          //转成json对象
          val jsonObj = JSON.parseObject(result)
          //获取位置信息
          val regeocode = jsonObj.getJSONObject("regeocode")
          if (regeocode != null && !regeocode.isEmpty) {
            val address = regeocode.getJSONObject("addressComponent")
            //获取省市区
            province = address.getString("province")
            city = address.getString("city")
          }
        }
        //将请求的结果赋值给bean
        bean.province = province
        bean.city = city
        //返回关联上维度的bean
        if (!it.hasNext) {
          //该分区对应的迭代器已经没有数据了，然后关闭连接
          httpclient.close()
        }
        bean
      })
      //关闭httpClient
      //httpclient.close()
      //返回一个新的迭代器

    })

    //触发Action
    //beanRDDWithLocation.foreach(println)
    //将数据写入到Hbase中
    beanRDDWithLocation.foreachPartition(it => {
      //创建Hbase连接
      val connection: Connection = ConnectionUtils.getConnection()
      val table = connection.getTable(TableName.valueOf("tb_order"))
      val puts = new util.ArrayList[Put](10)
      it.foreach(bean => {
        val put = new Put(Bytes.toBytes(bean.oid))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cid"), Bytes.toBytes(bean.cid))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("money"), Bytes.toBytes(bean.money))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("longitude"), Bytes.toBytes(bean.longitude))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("latitude"), Bytes.toBytes(bean.latitude))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("province"), Bytes.toBytes(bean.province))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("city"), Bytes.toBytes(bean.city))
        //将要写入的数据放入到put对象中
        //将put攒到puts集合中，批量写入
        puts.add(put)
        if(puts.size() == 10) {
          table.put(puts)
          puts.clear()
        }
      })
      table.put(puts)
      //关闭资源
      table.close()
      connection.close()
    })


    //5.释放资源
    sc.stop()


  }

}

case class OrderBean(oid: String,
                     cid: Int,
                     money: Double,
                     longitude: Double,
                     latitude: Double) {

  var province: String = _
  var city: String = _

  override def toString: String = s"$cid, $money, $longitude, $latitude, $province, $city"
}