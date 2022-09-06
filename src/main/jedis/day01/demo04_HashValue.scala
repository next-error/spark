package day01

import redis.clients.jedis.Jedis

import java.util

/**
 * 操作value为hash类型的数据
 * Map<String>,HashMap<String,String>
 *
 * 对小value累加
 *
 */
object demo04_HashValue {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("linux01", 6379)

    jedis.select(0)

    //判断小key是否存在
    val flag = jedis.hexists("江西", "南昌")
    println(flag)
    //若小key存在,则累加,不存在则set
    jedis.hincrByFloat("江西","赣州",5000)
    jedis.hincrByFloat("江西","南昌",5000)

    //返回大key对应的小map的size
    val len = jedis.hlen("江西")
    println(len)

    //设置大key的超时时间,Redis无法为每一个小key设置TTl(超时时间)
    jedis.expire("江西",30)//给大key设置超时时间,若超时,则其中的小k-v全部删除(存活时间30秒)


    //若key都存在,返回0,不存在返回1,并且创建
    val l = jedis.hsetnx("山东", "济南", "1000")
    println(l)

    jedis.close()
  }
}
