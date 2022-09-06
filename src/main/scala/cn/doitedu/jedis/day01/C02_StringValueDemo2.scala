package cn.doitedu.jedis.day01

import redis.clients.jedis.Jedis

/**
 * 演示Redis的Value是String类型的例子
 */
object C02_StringValueDemo2 {

  def main(args: Array[String]): Unit = {

    //1.创建Jedis连接
    val jedis = new Jedis("172.16.100.101", 6379)
    //2.由于设置了密码，要授权
    jedis.auth("123456")
    //3.选择相应的database
    jedis.select(1)

    //对key的value进行累加（只能针对于value是数字类型）
    //如果没有这个key，向相当于set，有就在原来的基础上进行累加
    jedis.incrBy("money", 5000)
    jedis.incr("money")
    //减
    jedis.decrBy("money", 1000)

    //累加double
    jedis.incrByFloat("money", 8.8)

    val r1 = jedis.get("money")

    println(r1)

    val r2 = jedis.strlen("money")
    println(r2)

    //判断指定的key是否存在
    val bool: Boolean = jedis.exists("abc")

    println(bool)


    //关闭连接
    jedis.close()


  }
}
