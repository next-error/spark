package cn.doitedu.jedis.day01

import java.util

import redis.clients.jedis.Jedis

/**
 * 操作Value是Hash类型的数据
 *
 * Map<String, HashMap<String, String>>
 *
 *
 */
object C03_HashValueDemo1 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")
    jedis.select(0)

    //如果读Hash类型的Value进行操作，方法是以h开头的
    jedis.hset("hunan", "changsha", "3000")
    jedis.hset("hunan", "zhuzhou", "4000")

    val v1: String = jedis.hget("hunan", "changsha")

    //获取全部的小keys
    val keys: util.Set[String] = jedis.hkeys("hunan")
    //变量java集合的隐式转换
    import scala.collection.JavaConverters._
    for(k <- keys.asScala) {
      println(k)
    }

    //获取全部的小values
    //val vals: util.List[String] = jedis.hvals("hunan")

    //获取全部的小key和小value
    val entries: util.Map[String, String] = jedis.hgetAll("hunan")
    for(tp <- entries.asScala) {
      val k = tp._1
      val v = tp._2
      println(s"k: $k, v: $v")
    }

    //删除一个小key和小value
    jedis.hdel("hunan", "zhuzhou")

    //关闭连接
    jedis.close()

  }
}
