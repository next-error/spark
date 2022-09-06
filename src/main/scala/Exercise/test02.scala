package Exercise

import org.apache.hadoop.shaded.org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients

object test02 {
  def main(args: Array[String]): Unit = {
    val key :String = "8e23a103dfe388ab3a5188c8a22d59cd"
    val longitude = 116.397128
    val latitude = 39.916527
    //初始化一个HttpClients
    val client = HttpClients.createDefault()

    //val httpGet = new HttpGet(s"https://restapi.amap.com/v3/assistant/coordinate/convert?locations=$longitude,$latitude&coordsys=gps&output=xml&key=$key")
    val httpGet = new HttpGet(s"https://restapi.amap.com/v3/geocode/geo?locations=$longitude,$latitude&region&output=xml&key=$key")
    val response: CloseableHttpResponse = client.execute(httpGet)//返回的是一个JSON
    println("res:"+response)

    val country = response.getLocale.getCountry
    //println(country)


  }

}
