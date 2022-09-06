package cn.doitedu.jedis.day02

import java.util

import redis.clients.jedis.Jedis

/**
 * 讲解redis的value是Set类型的（无序，元素不能重复）
 *
 * 并集、交集、差集
 *
 */
object C02_SetValueDemo2 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")

    //清空当前的DB
    jedis.flushDB()

    //向s1添加数据
    jedis.sadd("s1", "1", "2", "a", "b")

    //向s2添加数据
    jedis.sadd("s2", "a", "c", "1", "3")

    //对两个或多个set求并集
    val res1: util.Set[String] = jedis.sunion("s1", "s2")

    //对两个或多个set求交集
    val res2: util.Set[String] = jedis.sinter("s1", "s2")

    //对两个或多个set求差集
    val res3: util.Set[String] = jedis.sdiff("s1", "s2")



    //遍历set中的数据
    import scala.collection.JavaConverters._
    for(e <- res1.asScala) {
      println(e)
    }

    jedis.close()

  }

}
