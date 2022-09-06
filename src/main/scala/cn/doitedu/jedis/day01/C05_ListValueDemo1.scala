package cn.doitedu.jedis.day01

import java.util

import redis.clients.jedis.Jedis
import redis.clients.jedis.args.{ListDirection, ListPosition}
import redis.clients.jedis.params.SortingParams

/**
 * 演示value是List类型
 */
object C05_ListValueDemo1 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node-1.51doit.cn", 6379)
    jedis.auth("123456")
    jedis.select(2)

    //向List中放入数据
    //从前面插入
    jedis.lpush("lst2", "a", "b")
    //从后面插入
    jedis.rpush("lst2", "c", "d", "e", "f", "g")
    //b,a,c,d
    //将数据遍历出来
    jedis.lrange("lst2", 0, -1)
    //将数据从前面弹出,一个或多个
    jedis.lpop("lst2", 2)
    //将数据从后面弹出,一个或多个
    jedis.rpop("lst2", 1)

    //获取list的长度
    val r1: Long = jedis.llen("lst2")

    //在指定的元素前后或和面插入指定的数据
    //在c元素的后面插入f，如果有多个c，再第一次出现的c的后面
    jedis.linsert("lst2", ListPosition.AFTER, "c","f")

    jedis.linsert("lst2", ListPosition.BEFORE, "b","e")

    //将l2中的数据，从左边弹出来一个(lpop),然后添加到l1的右边（rpush）
    jedis.lmove("lst4", "lst3", ListDirection.LEFT, ListDirection.RIGHT)

    //放入数据
    jedis.rpush("lst5", "4", "5", "2", "3", "7", "6")
    val params = new SortingParams()
    params.desc()
    //将lst或set进行排序，将得到的结果以list形式返回
    val res: util.List[String] = jedis.sort("lst5", params)
    //将排序后的数据返回
    import scala.collection.JavaConverters._
    for(e <- res.asScala) {
      println(e)
    }

    //将排序后的结果放入到一个指定的list中
    jedis.sort("lst5", params, "lst6")

    jedis.close()

  }

}
