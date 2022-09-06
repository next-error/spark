package cn.doitedu.jedis.day02

import java.util

import redis.clients.jedis.Jedis
import redis.clients.jedis.resps.Tuple

/**
 * 讲解redis的value是TreeSet类型的（有序，元素不能重复）
 *
 */
object C03_ZSetValueDemo1 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")

    //清空当前的DB
    jedis.flushDB()

    //向s1添加数据,redis内部自动会按照得分的升序进行排序
    jedis.zadd("z1", 8.8, "sd")
    jedis.zadd("z1", 5.8, "hd")
    jedis.zadd("z1", 3.8, "ld")
    jedis.zadd("z1", 9.8, "hn")

    //将数据按照得分的升序遍历
    //从指定的下标开始遍历到指定的下标，如果结束的下标为-1，那么就是遍历全部
    //不包含得分
    val res1: util.List[String] = jedis.zrange("z1", 0, -1)

    //遍历数据的同时并将得分取出来,取最大的top3
    val res2: util.List[Tuple] = jedis.zrevrangeWithScores("z1", 0, 2)
    import scala.collection.JavaConverters._
    for (tp <- res2.asScala) {
      val element = tp.getElement
      val score = tp.getScore
      println(s"ele: $element, score: $score")
    }

    //删除元素
    jedis.zrem("z1", "hn")
    //判断一个元素是否在指定的zset中 ???n TODO

    //对指定元素的得分进行累加，累加后，会自动按照得分排序
    jedis.zincrby("z1", 100, "sd")


    //jedis.zunionstore()



    jedis.close()

  }

}
