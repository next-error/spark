package cn.doitedu.day03

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpClientTest {

  val key = "74e962e2f795114980cd33ce09d923d1"

  def main(args: Array[String]): Unit = {

    val longitude = 116.310003
    val latitude = 38.991957

    //初始化一个httpClient的客户端
    val httpclient = HttpClients.createDefault
    //REST请求方式GET POST DELETE PUT
    //GET查询
    // PUT、POST修改、添加
    // DELETE删除
    //发送查询情况，使用GET方式，new HttpGet
    val httpGet = new HttpGet(s"https://restapi.amap.com/v3/geocode/regeo?&location=$longitude,$latitude&key=$key")
    //发送http请求，进行查询
    val response1 = httpclient.execute(httpGet)

    try {
      //System.out.println(response1.getStatusLine)
      //获取返回结果
      val entity1 = response1.getEntity
      // do something useful with the response body
      // and ensure it is fully consumed

      var province: String = null
      var city: String = null
      if (response1.getStatusLine.getStatusCode == 200) {
        //获取请求的json字符串
        val result = EntityUtils.toString(entity1)
        println(result)
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

      println(province + ", " + city)

    } finally {
      response1.close()
    }

  }


}