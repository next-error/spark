package day02

import redis.clients.jedis.Jedis

import java.util

/**
 * value 为 Set 类型(无序,不重复)
 */
object demo02_SetValue {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("linux01", 6379)
    jedis.select(1)
    jedis.flushDB()

    jedis.sadd("s1","1","2","3","a","b")
    jedis.sadd("s2","c","b","5","a","b")

    //求并集
    val res1: util.Set[String] = jedis.sunion("s1", "s2")

    //遍历
    import scala.collection.JavaConverters._
    for(e <- res1.asScala){
      println(e)
    }
    println("=============================")

    //求差集
    val res2 = jedis.sdiff("s1", "s2")
    for(e <- res2.asScala){
      println(e)
    }
    println("=============================")

    //求并集
    val res3 = jedis.sinter("s1", "s2")
    for(e <- res3.asScala){
      println(e)
    }
    println("=============================")



    jedis.close()






  }
}
