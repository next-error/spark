package cn.doitedu.jedis.day01

import java.util

import redis.clients.jedis.Jedis

/**
 * 操作Value是Hash类型的数据
 *     大Key  ,         小Key， 小Value
 * Map<String, HashMap<String, String>>
 *
 * 对小key的value
 *
 */
object C04_HashValueDemo2 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")
    jedis.select(0)

    //判断小key是否存在
    val flag = jedis.hexists("hunan", "changsha")
    println(flag)

    //如果对对应的小key，就在原来基础上进行累加，否则就是hset
    jedis.hincrByFloat("hunan", "xiangtan", 888.88)

    //返回大key对应的小map的size
    val len = jedis.hlen("hunan")
    println(len)

    //向指定的大key，存储小key小value，如果对应的小key存在，就返回0，不存在返回1
    val r3: Long = jedis.hsetnx("shandong", "binzhou", "30000")

    //设置大key的超时时间，redis没法为每个小key设置TTL
    jedis.expire("hunan", 30) //给大key实在超时时间，如果超时了，该大key对应的全部小key全部删除

    //关闭连接
    jedis.close()
    
  }
}
