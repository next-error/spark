package day02

import redis.clients.jedis.Jedis

import java.util

/**
 * value 为 Set 类型(无序,不重复)
 */
object demo01_SetValue {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("linux01", 6379)
    jedis.select(1)
    jedis.flushDB()

    jedis.sadd("s1","1","2","3","a","b")
    jedis.sadd("s1","c","b","5","a","b")
    //遍历
    val res: util.Set[String] = jedis.smembers("s1")
    import scala.collection.JavaConverters._
    for(e <- res.asScala){
      println(e)
    }

  //删除
    jedis.srem("s1","1","z")
    //判断一个元素是否存在
    val flag = jedis.smismember("s1", "1")
    println(flag)

    jedis.close()






  }
}
