package day01

import redis.clients.jedis.Jedis

/**
 * 演示Redis的Value是String类型
 * 1.pom文件导入依赖
 * 2.
 */
object demo01_StringValue {
  def main(args: Array[String]): Unit = {

    //1.创建jedis链接
    val jedis = new Jedis("192.168.220.134", 6379)

    //2.输入密码
    //jedis.auth("123456")

    //3.选择相应的database
    jedis.select(1)

    //4.set 数据
    jedis.set("186","123456")
    //设置数据指定超时时间
    jedis.setex("158",30,"123")

    //5.get
    val r1 = jedis.get("186")
    println(r1)
    val r2 = jedis.get("158")
    println(r2)

    //6.del
    jedis.del("186")

    //7.关闭链接
    jedis.close()






  }
}
