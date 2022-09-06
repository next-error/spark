package cn.doitedu.jedis.day02

import java.util

import redis.clients.jedis.Jedis

/**
 * 讲解redis的value是Set类型的（无序，元素不能重复）
 */
object C01_SetValueDemo1 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")

    //清空当前的DB
    jedis.flushDB()

    //添加数据
    jedis.sadd("s1", "1", "2", "a", "b")

    //再添加元素,重复的数据会覆盖
    jedis.sadd("s1", "a", "c", "3")

    //遍历set中的数据
    val res: util.Set[String] = jedis.smembers("s1")
    import scala.collection.JavaConverters._
    for(e <- res.asScala) {
      println(e)
    }

    //删除一个或多个元素
    jedis.srem("s1", "c", "w")

    //判断一个元素是否在指定的set中
    val flag: Boolean = jedis.sismember("s1", "2")
    println(flag)


    jedis.close()

  }

}
