package day01

import redis.clients.jedis.Jedis

/**
 * 演示Redis的Value是String类型
 * 1.pom文件导入依赖
 * 2.
 */
object demo02_StringValue {
  def main(args: Array[String]): Unit = {

    //1.创建jedis链接
    val jedis = new Jedis("192.168.220.134", 6379)

    //2.输入密码
    //jedis.auth("123456")

    //3.选择相应的database
    jedis.select(1)

    //4.set 数据
    jedis.set("name","张三")
    //incrBy 如果没有这个key,就相当set,有就会在原来基础上累加
    jedis.incrBy("money",5000)

    jedis.decrBy("money",1000)
    val r1 = jedis.get("money")
    println(r1)


    //7.关闭链接
    jedis.close()






  }
}
