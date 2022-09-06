package cn.doitedu.jedis.day01

import redis.clients.jedis.Jedis

/**
 * 演示Redis的Value是String类型的例子
 *
 * 1.在pom.xml中导入依赖
 * <dependency>
 *    <groupId>redis.clients</groupId>
 *    <artifactId>jedis</artifactId>
 *    <version>4.2.3</version>
 * </dependency>
 */
object C01_StringValueDemo1 {

  def main(args: Array[String]): Unit = {

    //1.创建Jedis连接
    val jedis = new Jedis("172.16.100.101", 6379)
    //2.由于设置了密码，要授权
    jedis.auth("123456")
    //3.选择相应的database
    jedis.select(1)
    //4.set数据
    jedis.set("186", "123456")
    //设置数据，并且指定超时时间
    jedis.setex("158", 30, "123123")
    //取数据
    val r1 = jedis.get("186")
    println(r1)
    val r2 = jedis.get("158")
    println(r2)

    //删除数据
    jedis.del("186")

    //关闭连接
    jedis.close()


  }
}
