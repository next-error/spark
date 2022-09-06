package day02

import redis.clients.jedis.Jedis
import redis.clients.jedis.resps.Tuple
import untils.JedisConnectionPool

import java.util

/**
 * value 为 Set 类型(无序,不重复)
 */
object demo03_ZSetValue {
  def main(args: Array[String]): Unit = {
    val jedis = JedisConnectionPool.getJedis()
    jedis.select(1)
    jedis.flushDB()
    //自动按照得分的升序排列
    jedis.zadd("k1",8.8,"shangdong")
    jedis.zadd("k1",5.8,"zhejiang")
    jedis.zadd("k1",4.8,"henan")
    jedis.zadd("k1",9.8,"jiangsu")

    //按照sorce的升序遍历(不包含得分)
    val res1: util.List[String] = jedis.zrange("k1", 0, -1)
    //遍历的同时取出得分,降序(取出top3)
    val res2: util.List[Tuple] = jedis.zrevrangeWithScores("k1", 0, 2)

    import scala.collection.JavaConverters._
    for(e <- res1.asScala){
      println(e)
    }
    println("____________________________________")
    for(tp <- res2.asScala){
      val element = tp.getElement
      val score = tp.getScore
      println(s"element:$element,score$score")
    }

    //删除元素
    jedis.zrem("k1","shangdong")
    //对指定元素的score进行累加,累加完成自动排序
    jedis.zincrby("k1",100,"henan")

    //使用连接池时,close方法没有将其关闭,而是归还
    jedis.close()






  }
}
