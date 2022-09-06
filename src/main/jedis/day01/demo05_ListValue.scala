package day01

import org.apache.spark.sql.connector.catalog.TableChange.After
import redis.clients.jedis.Jedis

/**
 * value为 List 类型
 */
object demo05_ListValue {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("linux01", 6379)
    //jedis.auth("123456")

    jedis.select(2)

    //从前面插入数据
    jedis.lpush("lst1","a","b")
    //从后面插入数据
    jedis.rpush("lst1","c","d")
    //b,a,c,d
    //从指定元素前后插入数据
    //jedis.linsert("lst1",ListPostion,"a","f")

    //数据全部遍历出来
    val strings = jedis.lrange("lst1", 0, -1)
    println(strings)
    //将数据从前面弹出一个或多个
    jedis.lpop("lst1",2)
    //数据从后面弹出一个或多个
    jedis.rpop("lst1",1)

    //获取lst长度
    jedis.llen("lst1")

    //jedis.lmove()
    jedis.close()
  }



}
