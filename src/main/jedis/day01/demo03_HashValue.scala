package day01

import redis.clients.jedis.Jedis

import java.util

/**
 * 操作value为hash类型的数据
 * Map<String>,HashMap<String,String>
 *
 *
 */
object demo03_HashValue {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("linux01", 6379)

    jedis.select(0)

    //如果对Hash类型的value进行操作,方法是以h开头的
    jedis.hset("河北","邯郸","3000")
    jedis.hset("湖南","长沙","3100")
    jedis.hset("江西","南昌","4000")
    jedis.hset("江西","九江","4000")
    jedis.hset("江西","吉安","4000")

    val v1 = jedis.hget("江西", "南昌")
    println(v1)
    //获取全部的小keys
    val keys1: util.Set[String] = jedis.hkeys("江西")
    //遍历集合需要隐士转换(java集合转scala集合)
    import scala.collection.JavaConverters._
    for(k <- keys1.asScala ){
      println(k)
    }


    //获取全部小values
    val vals1: util.List[String] = jedis.hvals("江西")

    //获取全部的小key和小value
    val entries: util.Map[String, String] = jedis.hgetAll("江西")
    for( tp <- entries.asScala){
      val k = tp._1
      val v = tp._2
      println(s"k:$k, v:$v")
    }

    //删除一个小key和小value
    jedis.hdel("河北","邯郸")
  }
}
